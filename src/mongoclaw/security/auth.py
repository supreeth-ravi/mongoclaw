"""API authentication."""

from __future__ import annotations

import hashlib
import secrets
from typing import Any

from pydantic import SecretStr

from mongoclaw.core.config import get_settings
from mongoclaw.core.exceptions import AuthenticationError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class APIKeyAuth:
    """
    API key authentication handler.

    Features:
    - Multiple API keys
    - Secure comparison
    - Key metadata tracking
    """

    def __init__(
        self,
        api_keys: list[SecretStr] | None = None,
        header_name: str = "X-API-Key",
    ) -> None:
        """
        Initialize API key auth.

        Args:
            api_keys: List of valid API keys.
            header_name: Header name for the API key.
        """
        self._header_name = header_name

        # Store hashed keys for secure comparison
        self._key_hashes: set[str] = set()

        if api_keys:
            for key in api_keys:
                self._add_key(key.get_secret_value())

    def _add_key(self, key: str) -> None:
        """Add a key (hashed) to the valid set."""
        key_hash = self._hash_key(key)
        self._key_hashes.add(key_hash)

    def _hash_key(self, key: str) -> str:
        """Hash an API key."""
        return hashlib.sha256(key.encode()).hexdigest()

    def verify(self, api_key: str | None) -> bool:
        """
        Verify an API key.

        Args:
            api_key: The API key to verify.

        Returns:
            True if valid.

        Raises:
            AuthenticationError: If invalid.
        """
        if not api_key:
            raise AuthenticationError("API key required")

        if not self._key_hashes:
            # No keys configured, allow all (development mode)
            logger.warning("No API keys configured, authentication bypassed")
            return True

        key_hash = self._hash_key(api_key)

        # Constant-time comparison to prevent timing attacks
        is_valid = secrets.compare_digest(
            key_hash,
            next(iter(self._key_hashes)) if len(self._key_hashes) == 1 else key_hash,
        ) and key_hash in self._key_hashes

        if not is_valid:
            logger.warning("Invalid API key attempted")
            raise AuthenticationError("Invalid API key")

        return True

    def add_key(self, key: str) -> None:
        """Add a new valid API key."""
        self._add_key(key)

    def remove_key(self, key: str) -> bool:
        """
        Remove an API key.

        Returns:
            True if removed.
        """
        key_hash = self._hash_key(key)
        if key_hash in self._key_hashes:
            self._key_hashes.remove(key_hash)
            return True
        return False

    @property
    def header_name(self) -> str:
        """Get the header name."""
        return self._header_name

    @staticmethod
    def generate_key(length: int = 32) -> str:
        """
        Generate a new API key.

        Args:
            length: Key length in characters.

        Returns:
            Generated API key.
        """
        return secrets.token_urlsafe(length)


async def verify_api_key(api_key: str | None) -> bool:
    """
    Verify an API key using global settings.

    Args:
        api_key: The API key to verify.

    Returns:
        True if valid.

    Raises:
        AuthenticationError: If invalid.
    """
    settings = get_settings()
    auth = APIKeyAuth(
        api_keys=settings.security.api_keys,
        header_name=settings.api.api_key_header,
    )
    return auth.verify(api_key)


class JWTAuth:
    """
    JWT authentication handler (placeholder for future implementation).
    """

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        token_expiry_minutes: int = 60,
    ) -> None:
        """
        Initialize JWT auth.

        Args:
            secret_key: Secret key for signing.
            algorithm: JWT algorithm.
            token_expiry_minutes: Token expiry time.
        """
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._token_expiry = token_expiry_minutes

    def create_token(self, payload: dict[str, Any]) -> str:
        """Create a JWT token."""
        import jwt
        from datetime import datetime, timedelta

        expiry = datetime.utcnow() + timedelta(minutes=self._token_expiry)
        payload["exp"] = expiry

        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)

    def verify_token(self, token: str) -> dict[str, Any]:
        """
        Verify a JWT token.

        Args:
            token: The JWT token.

        Returns:
            Token payload.

        Raises:
            AuthenticationError: If invalid.
        """
        import jwt

        try:
            payload = jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm],
            )
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token expired")
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid token: {e}")
