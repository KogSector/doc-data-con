"""
Authentication middleware for FastAPI services.

Validates JWT tokens by calling the auth-middleware service.
Supports auth bypass for development mode.
"""

from dataclasses import dataclass, field
from typing import Optional

import httpx
import structlog
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = structlog.get_logger()
security = HTTPBearer(auto_error=False)


@dataclass
class AuthenticatedUser:
    """Authenticated user extracted from JWT or API key."""

    id: str
    email: str
    name: Optional[str] = None
    picture: Optional[str] = None
    roles: list[str] = field(default_factory=list)
    workspace_id: Optional[str] = None

    def has_role(self, role: str) -> bool:
        return role in self.roles


class AuthMiddleware:
    """
    FastAPI dependency that validates authentication.

    Usage:
        auth = AuthMiddleware()

        @app.get("/protected")
        async def protected(user: AuthenticatedUser = Depends(auth.required)):
            return {"user": user.id}
    """

    def __init__(
        self,
        auth_service_url: Optional[str] = None,
        auth_grpc_url: Optional[str] = None,
    ):
        from app.config import get_settings

        settings = get_settings()
        self.auth_service_url = auth_service_url or getattr(
            settings, "auth_service_url", "http://localhost:3010"
        )

        self._client = httpx.AsyncClient(timeout=5.0)

    async def _verify_token(self, token: str) -> AuthenticatedUser:
        """Validate a JWT token via auth-middleware."""
        try:
            resp = await self._client.post(
                f"{self.auth_service_url}/auth/validate",
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Token validation failed: {resp.status_code}",
                )
            data = resp.json()
            return AuthenticatedUser(
                id=data.get("id", ""),
                email=data.get("email", ""),
                name=data.get("name"),
                picture=data.get("picture"),
                roles=data.get("roles", []),
                workspace_id=data.get("workspace_id"),
            )
        except httpx.RequestError as e:
            logger.error("Auth service unreachable", error=str(e))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Authentication service unavailable",
            )

    async def _verify_api_key(self, key: str) -> AuthenticatedUser:
        """Validate an API key via auth-middleware."""
        try:
            resp = await self._client.post(
                f"{self.auth_service_url}/auth/validate-api-key",
                headers={"X-API-Key": key},
            )
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key",
                )
            data = resp.json()
            return AuthenticatedUser(
                id=data.get("user_id", data.get("id", "")),
                email=data.get("email", "api-key@confuse.dev"),
                name=data.get("name"),
                roles=data.get("scopes", data.get("roles", [])),
                workspace_id=data.get("workspace_id"),
            )
        except httpx.RequestError as e:
            logger.error("Auth service unreachable", error=str(e))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Authentication service unavailable",
            )

    async def get_internal_token(self, api_key: str, user_id: str, provider: str) -> dict:
        """
        Get auth token for a provider (internal call).
        """
        try:
            resp = await self._client.post(
                f"{self.auth_service_url}/api/auth/internal/tokens",
                json={"userId": user_id, "provider": provider},
                headers={"X-Api-Key": api_key},
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.error(
                    "Auth service returned error for token retrieval",
                    status=resp.status_code,
                    body=resp.text,
                )
                return {"success": False, "error": f"Status {resp.status_code}"}
        except Exception as e:
            logger.error("Auth service unreachable for token retrieval", error=str(e))
            return {"success": False, "error": str(e)}

    async def required(
        self,
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> AuthenticatedUser:
        """Dependency: require authentication (raises 401 if missing)."""
        # Try Bearer token
        if credentials and credentials.credentials:
            user = await self._verify_token(credentials.credentials)
            # Check for workspace header
            ws_id = request.headers.get("x-workspace-id")
            if ws_id:
                user.workspace_id = ws_id
            return user

        # Try API key header
        api_key = request.headers.get("x-api-key")
        if api_key:
            user = await self._verify_api_key(api_key)
            ws_id = request.headers.get("x-workspace-id")
            if ws_id:
                user.workspace_id = ws_id
            return user

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No authentication provided",
        )

    async def optional(
        self,
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> Optional[AuthenticatedUser]:
        """Dependency: optional authentication (returns None if missing)."""
        try:
            if credentials and credentials.credentials:
                user = await self._verify_token(credentials.credentials)
                ws_id = request.headers.get("x-workspace-id")
                if ws_id:
                    user.workspace_id = ws_id
                return user
        except HTTPException:
            pass

        return None


# Convenience instance
_default_auth = AuthMiddleware()
