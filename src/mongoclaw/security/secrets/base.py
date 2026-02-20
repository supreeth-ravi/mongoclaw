"""Abstract base class for secrets backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from mongoclaw.core.exceptions import SecretNotFoundError


class SecretsBackendBase(ABC):
    """
    Abstract base class for secrets backend implementations.

    Secrets backends retrieve sensitive values like API keys,
    database credentials, etc. from secure storage.
    """

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Get the backend name."""
        pass

    @abstractmethod
    async def get_secret(self, reference: str) -> str:
        """
        Retrieve a secret value.

        Args:
            reference: The secret reference/path.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If secret not found.
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the backend is healthy.

        Returns:
            True if healthy.
        """
        pass

    async def get_secret_optional(self, reference: str) -> str | None:
        """
        Retrieve a secret, returning None if not found.

        Args:
            reference: The secret reference/path.

        Returns:
            The secret value or None.
        """
        try:
            return await self.get_secret(reference)
        except SecretNotFoundError:
            return None

    async def get_secrets(self, references: list[str]) -> dict[str, str]:
        """
        Retrieve multiple secrets.

        Args:
            references: List of secret references.

        Returns:
            Dictionary of reference -> value.

        Raises:
            SecretNotFoundError: If any secret not found.
        """
        results = {}
        for ref in references:
            results[ref] = await self.get_secret(ref)
        return results


class SecretsResolver:
    """
    Resolves secret references in configuration values.

    Supports syntax like: ${secret:path/to/secret}
    """

    SECRET_PATTERN = r"\$\{secret:([^}]+)\}"

    def __init__(self, backend: SecretsBackendBase) -> None:
        self._backend = backend

    async def resolve(self, value: str) -> str:
        """
        Resolve secret references in a string.

        Args:
            value: String potentially containing secret references.

        Returns:
            String with secrets resolved.
        """
        import re

        async def replace_match(match: re.Match[str]) -> str:
            ref = match.group(1)
            return await self._backend.get_secret(ref)

        # Find all matches
        pattern = re.compile(self.SECRET_PATTERN)
        matches = list(pattern.finditer(value))

        if not matches:
            return value

        # Resolve all secrets
        result = value
        for match in reversed(matches):  # Reverse to preserve positions
            secret_value = await self._backend.get_secret(match.group(1))
            result = result[: match.start()] + secret_value + result[match.end() :]

        return result

    async def resolve_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve secret references in a dictionary.

        Args:
            data: Dictionary with potential secret references.

        Returns:
            Dictionary with secrets resolved.
        """
        result = {}

        for key, value in data.items():
            if isinstance(value, str):
                result[key] = await self.resolve(value)
            elif isinstance(value, dict):
                result[key] = await self.resolve_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    await self.resolve(v) if isinstance(v, str) else v for v in value
                ]
            else:
                result[key] = value

        return result
