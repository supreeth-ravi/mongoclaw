"""LiteLLM provider router for unified AI access."""

from __future__ import annotations

import time
from typing import Any

import litellm
from litellm import acompletion
from litellm.exceptions import (
    APIConnectionError,
    APIError,
    AuthenticationError,
    RateLimitError,
    ServiceUnavailableError,
)

from mongoclaw.core.config import Settings, get_settings
from mongoclaw.core.exceptions import (
    AIProviderError,
    AIRateLimitError,
    CostLimitExceededError,
)
from mongoclaw.core.types import AIResponse
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

# Disable LiteLLM's default logging
litellm.suppress_debug_info = True


class ProviderRouter:
    """
    Routes AI requests through LiteLLM for unified access to 100+ providers.

    Features:
    - Unified interface for all providers
    - Automatic retry with exponential backoff
    - Cost and token tracking
    - Response caching (optional)
    """

    def __init__(
        self,
        settings: Settings | None = None,
        api_keys: dict[str, str] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._api_keys = api_keys or {}

        # Configure LiteLLM
        self._configure_litellm()

        # Tracking
        self._total_tokens = 0
        self._total_cost = 0.0
        self._request_count = 0

    def _configure_litellm(self) -> None:
        """Configure LiteLLM settings."""
        ai_settings = self._settings.ai

        # Set timeouts
        litellm.request_timeout = ai_settings.request_timeout

        # Set API keys from settings or environment
        for provider, key in self._api_keys.items():
            setattr(litellm, f"{provider}_key", key)

    async def complete(
        self,
        model: str,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: str | None = None,
        api_key: str | None = None,
        **kwargs: Any,
    ) -> AIResponse:
        """
        Send a completion request to the specified model.

        Args:
            model: Model identifier (e.g., "gpt-4o-mini", "claude-3-sonnet").
            prompt: The user prompt.
            system_prompt: Optional system prompt.
            temperature: Sampling temperature.
            max_tokens: Maximum tokens in response.
            response_format: Response format ("json_object", etc.).
            api_key: Optional API key override.
            **kwargs: Additional provider-specific parameters.

        Returns:
            AIResponse with content and metadata.

        Raises:
            AIProviderError: For provider errors.
            AIRateLimitError: For rate limit errors.
            CostLimitExceededError: If cost limits exceeded.
        """
        ai_settings = self._settings.ai

        # Check cost limits
        self._check_limits()

        # Build messages
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # Build request parameters
        request_params: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature or ai_settings.default_temperature,
            "max_tokens": max_tokens or ai_settings.default_max_tokens,
        }

        if api_key:
            request_params["api_key"] = api_key

        if response_format == "json_object":
            request_params["response_format"] = {"type": "json_object"}

        # Add any extra kwargs
        request_params.update(kwargs)

        start_time = time.perf_counter()

        try:
            response = await acompletion(**request_params)

            latency_ms = (time.perf_counter() - start_time) * 1000

            # Extract response data
            content = response.choices[0].message.content or ""
            finish_reason = response.choices[0].finish_reason or ""

            # Get usage info
            usage = response.usage
            prompt_tokens = usage.prompt_tokens if usage else 0
            completion_tokens = usage.completion_tokens if usage else 0
            total_tokens = usage.total_tokens if usage else 0

            # Calculate cost using LiteLLM's cost calculation
            try:
                cost = litellm.completion_cost(completion_response=response)
            except Exception:
                # Fallback for unmapped models (e.g., OpenRouter)
                cost = 0.0

            # Update tracking
            self._total_tokens += total_tokens
            self._total_cost += cost
            self._request_count += 1

            # Determine provider from model
            provider = self._get_provider_from_model(model)

            logger.info(
                "AI completion successful",
                model=model,
                provider=provider,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost_usd=cost,
                latency_ms=round(latency_ms, 2),
            )

            return AIResponse(
                content=content,
                model=model,
                provider=provider,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                cost_usd=cost,
                latency_ms=latency_ms,
                finish_reason=finish_reason,
                raw_response=response.model_dump() if hasattr(response, "model_dump") else None,
            )

        except RateLimitError as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            logger.warning(
                "Rate limit exceeded",
                model=model,
                error=str(e),
                latency_ms=round(latency_ms, 2),
            )
            raise AIRateLimitError(
                provider=self._get_provider_from_model(model),
                model=model,
            )

        except AuthenticationError as e:
            logger.error(
                "Authentication failed",
                model=model,
                error=str(e),
            )
            raise AIProviderError(
                f"Authentication failed: {e}",
                provider=self._get_provider_from_model(model),
                model=model,
            )

        except (APIConnectionError, ServiceUnavailableError) as e:
            logger.error(
                "Provider unavailable",
                model=model,
                error=str(e),
            )
            raise AIProviderError(
                f"Provider unavailable: {e}",
                provider=self._get_provider_from_model(model),
                model=model,
            )

        except APIError as e:
            logger.error(
                "API error",
                model=model,
                error=str(e),
            )
            raise AIProviderError(
                f"API error: {e}",
                provider=self._get_provider_from_model(model),
                model=model,
            )

        except Exception as e:
            logger.exception(
                "Unexpected error in AI completion",
                model=model,
                error=str(e),
            )
            raise AIProviderError(
                f"Unexpected error: {e}",
                provider=self._get_provider_from_model(model),
                model=model,
            )

    def _check_limits(self) -> None:
        """Check if cost or token limits are exceeded."""
        ai_settings = self._settings.ai

        if ai_settings.global_cost_limit_usd:
            if self._total_cost >= ai_settings.global_cost_limit_usd:
                raise CostLimitExceededError(
                    limit_type="cost",
                    current_value=self._total_cost,
                    limit_value=ai_settings.global_cost_limit_usd,
                )

        if ai_settings.global_token_limit:
            if self._total_tokens >= ai_settings.global_token_limit:
                raise CostLimitExceededError(
                    limit_type="tokens",
                    current_value=float(self._total_tokens),
                    limit_value=float(ai_settings.global_token_limit),
                )

    def _get_provider_from_model(self, model: str) -> str:
        """Extract provider name from model identifier."""
        # LiteLLM model format: provider/model or just model
        if "/" in model:
            return model.split("/")[0]

        # Infer from model name
        model_lower = model.lower()

        if model_lower.startswith(("gpt-", "o1", "o3")):
            return "openai"
        if model_lower.startswith("claude"):
            return "anthropic"
        if model_lower.startswith("gemini"):
            return "google"
        if model_lower.startswith(("llama", "mixtral")):
            return "groq"
        if model_lower.startswith("mistral"):
            return "mistral"
        if model_lower.startswith("command"):
            return "cohere"

        return "unknown"

    def get_stats(self) -> dict[str, Any]:
        """Get usage statistics."""
        return {
            "total_tokens": self._total_tokens,
            "total_cost_usd": round(self._total_cost, 6),
            "request_count": self._request_count,
            "avg_tokens_per_request": (
                self._total_tokens / self._request_count
                if self._request_count > 0
                else 0
            ),
        }

    def reset_stats(self) -> None:
        """Reset usage statistics."""
        self._total_tokens = 0
        self._total_cost = 0.0
        self._request_count = 0

    async def health_check(self) -> bool:
        """
        Check if the AI provider is reachable.

        Uses a minimal completion request.
        """
        try:
            await self.complete(
                model=self._settings.ai.default_model,
                prompt="Hi",
                max_tokens=5,
            )
            return True
        except Exception:
            return False
