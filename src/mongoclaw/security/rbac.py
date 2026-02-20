"""Role-based access control."""

from __future__ import annotations

from enum import Enum
from typing import Any

from mongoclaw.core.exceptions import AuthorizationError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class Permission(str, Enum):
    """Available permissions."""

    # Agent permissions
    AGENT_READ = "agent:read"
    AGENT_CREATE = "agent:create"
    AGENT_UPDATE = "agent:update"
    AGENT_DELETE = "agent:delete"
    AGENT_ENABLE = "agent:enable"
    AGENT_DISABLE = "agent:disable"

    # Execution permissions
    EXECUTION_READ = "execution:read"
    EXECUTION_RETRY = "execution:retry"
    EXECUTION_CANCEL = "execution:cancel"

    # Queue permissions
    QUEUE_READ = "queue:read"
    QUEUE_PURGE = "queue:purge"
    DLQ_READ = "dlq:read"
    DLQ_RETRY = "dlq:retry"
    DLQ_DELETE = "dlq:delete"

    # System permissions
    SYSTEM_ADMIN = "system:admin"
    SYSTEM_HEALTH = "system:health"
    SYSTEM_METRICS = "system:metrics"
    SYSTEM_CONFIG = "system:config"


class Role:
    """
    Role with a set of permissions.
    """

    def __init__(
        self,
        name: str,
        permissions: set[Permission],
        description: str = "",
    ) -> None:
        """
        Initialize role.

        Args:
            name: Role name.
            permissions: Set of permissions.
            description: Role description.
        """
        self.name = name
        self.permissions = permissions
        self.description = description

    def has_permission(self, permission: Permission) -> bool:
        """Check if role has a permission."""
        # Admin has all permissions
        if Permission.SYSTEM_ADMIN in self.permissions:
            return True
        return permission in self.permissions

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "permissions": [p.value for p in self.permissions],
            "description": self.description,
        }


# Predefined roles
ROLE_ADMIN = Role(
    name="admin",
    permissions={Permission.SYSTEM_ADMIN},
    description="Full system access",
)

ROLE_OPERATOR = Role(
    name="operator",
    permissions={
        Permission.AGENT_READ,
        Permission.AGENT_ENABLE,
        Permission.AGENT_DISABLE,
        Permission.EXECUTION_READ,
        Permission.EXECUTION_RETRY,
        Permission.QUEUE_READ,
        Permission.DLQ_READ,
        Permission.DLQ_RETRY,
        Permission.SYSTEM_HEALTH,
        Permission.SYSTEM_METRICS,
    },
    description="Operational access for monitoring and basic management",
)

ROLE_DEVELOPER = Role(
    name="developer",
    permissions={
        Permission.AGENT_READ,
        Permission.AGENT_CREATE,
        Permission.AGENT_UPDATE,
        Permission.AGENT_DELETE,
        Permission.AGENT_ENABLE,
        Permission.AGENT_DISABLE,
        Permission.EXECUTION_READ,
        Permission.EXECUTION_RETRY,
        Permission.QUEUE_READ,
        Permission.DLQ_READ,
        Permission.DLQ_RETRY,
        Permission.SYSTEM_HEALTH,
    },
    description="Development access for creating and managing agents",
)

ROLE_VIEWER = Role(
    name="viewer",
    permissions={
        Permission.AGENT_READ,
        Permission.EXECUTION_READ,
        Permission.QUEUE_READ,
        Permission.DLQ_READ,
        Permission.SYSTEM_HEALTH,
        Permission.SYSTEM_METRICS,
    },
    description="Read-only access",
)

DEFAULT_ROLES = {
    "admin": ROLE_ADMIN,
    "operator": ROLE_OPERATOR,
    "developer": ROLE_DEVELOPER,
    "viewer": ROLE_VIEWER,
}


class RBACManager:
    """
    Role-based access control manager.

    Manages user-role assignments and permission checks.
    """

    def __init__(self) -> None:
        self._roles = DEFAULT_ROLES.copy()
        self._user_roles: dict[str, set[str]] = {}

    def add_role(self, role: Role) -> None:
        """Add a custom role."""
        self._roles[role.name] = role

    def get_role(self, name: str) -> Role | None:
        """Get a role by name."""
        return self._roles.get(name)

    def assign_role(self, user_id: str, role_name: str) -> None:
        """
        Assign a role to a user.

        Args:
            user_id: User identifier.
            role_name: Role name.
        """
        if role_name not in self._roles:
            raise ValueError(f"Unknown role: {role_name}")

        if user_id not in self._user_roles:
            self._user_roles[user_id] = set()

        self._user_roles[user_id].add(role_name)
        logger.info("Assigned role", user_id=user_id, role=role_name)

    def revoke_role(self, user_id: str, role_name: str) -> None:
        """Revoke a role from a user."""
        if user_id in self._user_roles:
            self._user_roles[user_id].discard(role_name)
            logger.info("Revoked role", user_id=user_id, role=role_name)

    def get_user_roles(self, user_id: str) -> list[Role]:
        """Get all roles for a user."""
        role_names = self._user_roles.get(user_id, set())
        return [self._roles[name] for name in role_names if name in self._roles]

    def get_user_permissions(self, user_id: str) -> set[Permission]:
        """Get all permissions for a user."""
        roles = self.get_user_roles(user_id)

        permissions: set[Permission] = set()
        for role in roles:
            permissions.update(role.permissions)

        return permissions

    def check_permission(
        self,
        user_id: str,
        permission: Permission,
        resource: str | None = None,
    ) -> bool:
        """
        Check if a user has a permission.

        Args:
            user_id: User identifier.
            permission: Permission to check.
            resource: Optional resource identifier.

        Returns:
            True if permitted.
        """
        permissions = self.get_user_permissions(user_id)

        # Admin has all permissions
        if Permission.SYSTEM_ADMIN in permissions:
            return True

        return permission in permissions

    def require_permission(
        self,
        user_id: str,
        permission: Permission,
        resource: str | None = None,
    ) -> None:
        """
        Require a permission, raising if not permitted.

        Args:
            user_id: User identifier.
            permission: Required permission.
            resource: Optional resource identifier.

        Raises:
            AuthorizationError: If not permitted.
        """
        if not self.check_permission(user_id, permission, resource):
            logger.warning(
                "Permission denied",
                user_id=user_id,
                permission=permission.value,
                resource=resource,
            )
            raise AuthorizationError(
                action=permission.value,
                resource=resource or "system",
            )

    def list_roles(self) -> list[dict[str, Any]]:
        """List all available roles."""
        return [role.to_dict() for role in self._roles.values()]
