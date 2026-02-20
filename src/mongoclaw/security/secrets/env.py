"""Environment variable secrets backend."""

from __future__ import annotations

import os

from mongoclaw.core.exceptions import SecretNotFoundError
from mongoclaw.observability.logging import get_logger
from mongoclaw.security.secrets.base import SecretsBackendBase

logger = get_logger(__name__)


class EnvSecretsBackend(SecretsBackendBase):
    """
    Secrets backend using environment variables.

    Reference format: VARIABLE_NAME
    Example: OPENAI_API_KEY
    """

    def __init__(self, prefix: str = "") -> None:
        """
        Initialize environment secrets backend.

        Args:
            prefix: Optional prefix for variable names.
        """
        self._prefix = prefix

    @property
    def backend_name(self) -> str:
        return "env"

    async def get_secret(self, reference: str) -> str:
        """Get a secret from environment variables."""
        var_name = f"{self._prefix}{reference}" if self._prefix else reference

        value = os.environ.get(var_name)

        if value is None:
            raise SecretNotFoundError(reference, self.backend_name)

        logger.debug("Retrieved secret from environment", reference=reference)
        return value

    async def health_check(self) -> bool:
        """Environment backend is always healthy."""
        return True
