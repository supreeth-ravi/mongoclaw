"""AWS Secrets Manager backend."""

from __future__ import annotations

import json
from typing import Any

from mongoclaw.core.exceptions import SecretNotFoundError
from mongoclaw.observability.logging import get_logger
from mongoclaw.security.secrets.base import SecretsBackendBase

logger = get_logger(__name__)


class AWSSecretsBackend(SecretsBackendBase):
    """
    Secrets backend using AWS Secrets Manager.

    Reference format: secret-name#key
    Example: prod/mongodb/credentials#password
    """

    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: str | None = None,
    ) -> None:
        """
        Initialize AWS Secrets Manager backend.

        Args:
            region_name: AWS region.
            profile_name: Optional AWS profile name.
        """
        self._region_name = region_name
        self._profile_name = profile_name
        self._client: Any = None

    @property
    def backend_name(self) -> str:
        return "aws"

    def _get_client(self) -> Any:
        """Get or create Secrets Manager client."""
        if self._client is None:
            import boto3

            session_kwargs: dict[str, Any] = {
                "region_name": self._region_name,
            }
            if self._profile_name:
                session_kwargs["profile_name"] = self._profile_name

            session = boto3.Session(**session_kwargs)
            self._client = session.client("secretsmanager")

        return self._client

    async def get_secret(self, reference: str) -> str:
        """
        Get a secret from AWS Secrets Manager.

        Reference format: secret-name#key
        If no key is specified, returns the entire secret string.
        """
        # Parse reference
        if "#" in reference:
            secret_name, key = reference.rsplit("#", 1)
        else:
            secret_name = reference
            key = None

        try:
            client = self._get_client()

            response = client.get_secret_value(SecretId=secret_name)

            if "SecretString" in response:
                secret_string = response["SecretString"]
            else:
                # Binary secret
                import base64

                secret_string = base64.b64decode(response["SecretBinary"]).decode()

            if key:
                # Parse as JSON and extract key
                try:
                    secret_data = json.loads(secret_string)
                    if key not in secret_data:
                        raise SecretNotFoundError(reference, self.backend_name)
                    return str(secret_data[key])
                except json.JSONDecodeError:
                    raise SecretNotFoundError(reference, self.backend_name)

            logger.debug("Retrieved secret from AWS", secret_name=secret_name)
            return secret_string

        except SecretNotFoundError:
            raise

        except Exception as e:
            error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if error_code in ("ResourceNotFoundException", "InvalidParameterException"):
                raise SecretNotFoundError(reference, self.backend_name)

            logger.error("AWS Secrets Manager error", error=str(e))
            raise SecretNotFoundError(reference, self.backend_name)

    async def health_check(self) -> bool:
        """Check AWS Secrets Manager connectivity."""
        try:
            client = self._get_client()
            # List secrets is a lightweight way to verify connectivity
            client.list_secrets(MaxResults=1)
            return True
        except Exception:
            return False

    async def create_secret(
        self,
        name: str,
        value: str | dict[str, Any],
        description: str = "",
    ) -> str:
        """
        Create a new secret.

        Args:
            name: Secret name.
            value: Secret value (string or dict).
            description: Optional description.

        Returns:
            Secret ARN.
        """
        client = self._get_client()

        secret_string = value if isinstance(value, str) else json.dumps(value)

        response = client.create_secret(
            Name=name,
            SecretString=secret_string,
            Description=description,
        )

        logger.info("Created AWS secret", name=name)
        return response["ARN"]

    async def update_secret(
        self,
        name: str,
        value: str | dict[str, Any],
    ) -> None:
        """
        Update an existing secret.

        Args:
            name: Secret name.
            value: New secret value.
        """
        client = self._get_client()

        secret_string = value if isinstance(value, str) else json.dumps(value)

        client.update_secret(
            SecretId=name,
            SecretString=secret_string,
        )

        logger.info("Updated AWS secret", name=name)

    async def delete_secret(
        self,
        name: str,
        force: bool = False,
    ) -> None:
        """
        Delete a secret.

        Args:
            name: Secret name.
            force: Force immediate deletion (no recovery).
        """
        client = self._get_client()

        kwargs: dict[str, Any] = {"SecretId": name}
        if force:
            kwargs["ForceDeleteWithoutRecovery"] = True

        client.delete_secret(**kwargs)
        logger.info("Deleted AWS secret", name=name, force=force)
