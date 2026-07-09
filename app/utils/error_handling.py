"""
Error handling middleware and utilities for Data Connector Service.
"""

import structlog
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_422_UNPROCESSABLE_ENTITY
import traceback
from app.utils.validation import ValidationError


logger = structlog.get_logger()


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Global error handling middleware."""

    async def dispatch(self, request: Request, call_next):
        """Process request and handle any exceptions."""
        try:
            response = await call_next(request)
            return response
        except HTTPException as exc:
            # Don't log client errors (4xx) as errors
            if exc.status_code < 500:
                logger.info(
                    "HTTP client error",
                    status_code=exc.status_code,
                    detail=exc.detail,
                    path=request.url.path,
                    method=request.method,
                )
            else:
                logger.error(
                    "HTTP server error",
                    status_code=exc.status_code,
                    detail=exc.detail,
                    path=request.url.path,
                    method=request.method,
                )
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "error": {
                        "message": exc.detail,
                        "type": "http_error",
                        "status_code": exc.status_code,
                    }
                },
            )
        except ValidationError as exc:
            logger.warning(
                "Validation error", error=str(exc), path=request.url.path, method=request.method
            )
            return JSONResponse(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                content={
                    "error": {
                        "message": str(exc),
                        "type": "validation_error",
                        "status_code": HTTP_422_UNPROCESSABLE_ENTITY,
                    }
                },
            )
        except Exception as exc:
            # Log the full error with traceback
            logger.error(
                "Unhandled exception",
                error=str(exc),
                traceback=traceback.format_exc(),
                path=request.url.path,
                method=request.method,
            )

            # Return generic error message to avoid leaking sensitive info
            return JSONResponse(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": {
                        "message": "Internal server error",
                        "type": "internal_error",
                        "status_code": HTTP_500_INTERNAL_SERVER_ERROR,
                    }
                },
            )


class ServiceError(Exception):
    """Base class for service-specific errors."""

    def __init__(self, message: str, error_code: str = None, details: dict = None):
        self.message = message
        self.error_code = error_code or "SERVICE_ERROR"
        self.details = details or {}
        super().__init__(self.message)


async def handle_service_error(request: Request, exc: ServiceError) -> JSONResponse:
    """Handle service-specific errors."""
    logger.error(
        "Service error",
        error_code=exc.error_code,
        message=exc.message,
        details=exc.details,
        path=request.url.path,
        method=request.method,
    )

    return JSONResponse(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "message": exc.message,
                "type": exc.error_code.lower(),
                "status_code": HTTP_500_INTERNAL_SERVER_ERROR,
                "details": exc.details,
            }
        },
    )


async def handle_validation_error(request: Request, exc: ValidationError) -> JSONResponse:
    """Handle validation errors."""
    logger.warning("Validation error", error=str(exc), path=request.url.path, method=request.method)

    return JSONResponse(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "message": str(exc),
                "type": "validation_error",
                "status_code": HTTP_422_UNPROCESSABLE_ENTITY,
            }
        },
    )


class RetryableError(ServiceError):
    """Errors that can be retried."""

    def __init__(self, message: str, retry_after: int = 60, details: dict = None):
        super().__init__(message, "RETRYABLE_ERROR", details)
        self.retry_after = retry_after


async def handle_retryable_error(request: Request, exc: RetryableError) -> JSONResponse:
    """Handle retryable errors with retry-after header."""
    logger.warning(
        "Retryable error",
        message=exc.message,
        retry_after=exc.retry_after,
        details=exc.details,
        path=request.url.path,
        method=request.method,
    )

    return JSONResponse(
        status_code=503,  # Service Unavailable
        headers={"Retry-After": str(exc.retry_after)},
        content={
            "error": {
                "message": exc.message,
                "type": "retryable_error",
                "status_code": 503,
                "retry_after": exc.retry_after,
                "details": exc.details,
            }
        },
    )


def setup_error_handlers(app):
    """Setup error handlers for the FastAPI app."""
    from app.utils.validation import ValidationError

    app.add_exception_handler(ServiceError, handle_service_error)
    app.add_exception_handler(RetryableError, handle_retryable_error)
    app.add_exception_handler(ValidationError, handle_validation_error)
