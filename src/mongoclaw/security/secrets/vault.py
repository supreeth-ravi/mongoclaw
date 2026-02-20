"""HashiCorp Vault secrets backend."""

from __future__ import annotations

from typing import Any

from mongoclaw.core.exceptions import SecretNotFoundError
from mongoclaw.observability.logging import get_logger
from mongoclaw.security.secrets.base import SecretsBackendBase

logger = get_logger(__name__)


class VaultSecretsBackend(SecretsBackendBase):
    """
    Secrets backend using HashiCorp Vault.

    Reference format: path/to/secret#key
    Example: database/creds/mongodb#password
    """

    def __init__(
        self,
        url: str,
        token: str | None = None,
        mount_point: str = "secret",
        namespace: str | None = None,
    ) -> None:
        """
        Initialize Vault secrets backend.

        Args:
            url: Vault server URL.
            token: Authentication token.
            mount_point: KV secrets engine mount point.
            namespace: Optional Vault namespace.
        """
        self._url = url
        self._token = token
        self._mount_point = mount_point
        self._namespace = namespace
        self._client: Any = None

    @property
    def backend_name(self) -> str:
        return "vault"

    def _get_client(self) -> Any:
        """Get or create Vault client."""
        if self._client is None:
            import hvac

            self._client = hvac.Client(
                url=self._url,
                token=self._token,
                namespace=self._namespace,
            )

        return self._client

    async def get_secret(self, reference: str) -> str:
        """
        Get a secret from Vault.

        Reference format: path/to/secret#key
        If no key is specified, returns the first value.
        """
        # Parse reference
        if "#" in reference:
            path, key = reference.rsplit("#", 1)
        else:
            path = reference
            key = None

        try:
            client = self._get_client()

            # Read from KV v2
            response = client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self._mount_point,
            )

            data = response.get("data", {}).get("data", {})

            if key:
                if key not in data:
                    raise SecretNotFoundError(reference, self.backend_name)
                value = data[key]
            else:
                # Return first value
                if not data:
                    raise SecretNotFoundError(reference, self.backend_name)
                value = next(iter(data.values()))

            logger.debug("Retrieved secret from Vault", path=path)
            return str(value)

        except SecretNotFoundError:
            raise

        except Exception as e:
            logger.error("Vault error", error=str(e), path=path)
            raise SecretNotFoundError(reference, self.backend_name)

    async def health_check(self) -> bool:
        """Check Vault connectivity."""
        try:
            client = self._get_client()
            return client.is_authenticated()
        except Exception:
            return False

    async def get_dynamic_credentials(
        self,
        path: str,
        role: str,
    ) -> dict[str, str]:
        """
        Get dynamic credentials from Vault.

        Args:
            path: Secrets engine path (e.g., "database").
            role: Role name for credential generation.

        Returns:
            Dictionary with credentials.
        """
        try:
            client = self._get_client()

            response = client.secrets.database.generate_credentials(
                name=role,
                mount_point=path,
            )

            return {
                "username": response["data"]["username"],
                "password": response["data"]["password"],
            }

        except Exception as e:
            logger.error(
                "Failed to get dynamic credentials",
                error=str(e),
                path=path,
                role=role,
            )
            raise
