"""
Rate limiting middleware for FastAPI services.

In-memory sliding window rate limiter with per-path limits
and standard X-RateLimit-* response headers.
"""

import os
import time
from collections import defaultdict
from typing import Optional

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

logger = structlog.get_logger()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter for FastAPI."""

    def __init__(
        self,
        app,
        default_limit: int = 100,
        search_limit: Optional[int] = None,
        sources_limit: Optional[int] = None,
        sync_limit: Optional[int] = None,
        window_secs: int = 60,
        skip_rate_limiting: Optional[bool] = None,
    ):
        super().__init__(app)
        self.default_limit = int(os.getenv("RATE_LIMIT_DEFAULT", str(default_limit)))
        self.search_limit = int(
            os.getenv("RATE_LIMIT_SEARCH", str(search_limit or default_limit // 2))
        )
        self.sources_limit = int(
            os.getenv("RATE_LIMIT_SOURCES", str(sources_limit or default_limit))
        )
        self.sync_limit = int(os.getenv("RATE_LIMIT_SYNC", str(sync_limit or default_limit // 5)))
        self.window_secs = window_secs
        if skip_rate_limiting is not None:
            self.skip = skip_rate_limiting
        else:
            self.skip = os.getenv("SKIP_RATE_LIMITING", "false").lower() == "true"
        # key -> list of timestamps
        self._counters: dict[str, list[float]] = defaultdict(list)

    def _get_limit_for_path(self, path: str) -> int:
        if "/search" in path:
            return self.search_limit
        if "/sources" in path:
            return self.sources_limit
        if "/sync" in path:
            return self.sync_limit
        return self.default_limit

    def _get_client_id(self, request: Request) -> str:
        # Try user from state (set by auth middleware)
        user = getattr(request.state, "user", None)
        if user and hasattr(user, "id"):
            return f"user:{user.id}"
        # Fallback to IP
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if self.skip:
            return await call_next(request)

        path = request.url.path

        # Skip health endpoints
        if path in ("/health", "/metrics", "/status"):
            return await call_next(request)

        client_id = self._get_client_id(request)
        limit = self._get_limit_for_path(path)
        key = f"ratelimit:{client_id}:{path.replace('/', '_')}"

        now = time.time()
        window_start = now - self.window_secs

        # Remove old entries
        timestamps = self._counters[key]
        self._counters[key] = [ts for ts in timestamps if ts > window_start]
        self._counters[key].append(now)

        count = len(self._counters[key])

        if count > limit:
            logger.warning(
                "Rate limit exceeded", client=client_id, path=path, count=count, limit=limit
            )
            return JSONResponse(
                status_code=429,
                content={"error": {"code": "RATE_LIMITED", "message": "Too many requests"}},
                headers={
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(now + self.window_secs)),
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, limit - count))
        response.headers["X-RateLimit-Reset"] = str(int(now + self.window_secs))
        return response
