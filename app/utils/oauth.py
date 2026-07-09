"""
OAuth utilities for data-connector service.

Centralizes OAuth credential handling, validation, and URL generation
across different providers (GitHub, GitLab, Bitbucket, etc.).
"""

from typing import Dict, Any, Optional
import structlog

logger = structlog.get_logger()


class OAuthCredentialManager:
    """Manages OAuth credentials and authentication across providers."""

    @staticmethod
    def validate_credentials(credentials: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate OAuth credentials dictionary.

        Args:
            credentials: Dictionary containing OAuth credentials

        Returns:
            Validated credentials dictionary

        Raises:
            ValueError: If credentials are invalid
        """
        if not isinstance(credentials, dict):
            raise ValueError("Credentials must be a dictionary")

        validated = {}

        # Common OAuth fields
        for key in ["access_token", "refresh_token", "client_id", "client_secret"]:
            if key in credentials:
                value = credentials[key]
                if not isinstance(value, str) or len(value) > 10000:
                    raise ValueError(f"Invalid {key} format")
                validated[key] = value.strip()

        return validated

    @staticmethod
    def get_authenticated_url(
        uri: str, provider: str, credentials: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate authenticated URL for Git operations.

        Args:
            uri: Original repository URI
            provider: Provider name (github, gitlab, bitbucket)
            credentials: OAuth credentials dictionary

        Returns:
            Authenticated URL for cloning/accessing repository
        """
        if not credentials or not credentials.get("access_token"):
            # Fall back to public access
            return uri

        access_token = credentials.get("access_token")
        url_obj = uri.replace("https://", "").replace("http://", "")

        if provider == "github":
            username = credentials.get("username", "oauth2")
            return f"https://{username}:{access_token}@{url_obj}"
        elif provider == "gitlab":
            return f"https://oauth2:{access_token}@{url_obj}"
        elif provider == "bitbucket":
            # Bitbucket uses x-token-auth
            return f"https://x-token-auth:{access_token}@{url_obj}"
        else:
            # Unsupported provider, fall back to public URI
            logger.warning(f"Unsupported provider for authenticated URL: {provider}")
            return uri

    @staticmethod
    def get_auth_headers(provider: str, credentials: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate authorization headers for API requests.

        Args:
            provider: Provider name
            credentials: OAuth credentials dictionary

        Returns:
            Dictionary of authorization headers
        """
        access_token = credentials.get("access_token")
        if not access_token:
            raise ValueError("Access token required for authorization headers")

        headers = {}

        if provider == "github":
            headers.update(
                {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                }
            )
        elif provider == "gitlab":
            headers.update(
                {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                }
            )
        elif provider == "bitbucket":
            headers.update(
                {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                }
            )
        else:
            # Generic Bearer token for other providers
            headers.update(
                {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                }
            )

        return headers

    @staticmethod
    def sanitize_credentials_for_logging(credentials: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize credentials for safe logging.

        Args:
            credentials: Raw credentials dictionary

        Returns:
            Sanitized credentials with sensitive data masked
        """
        sanitized = credentials.copy()

        # Mask sensitive fields
        sensitive_fields = ["access_token", "refresh_token", "client_secret"]
        for field in sensitive_fields:
            if field in sanitized:
                token = sanitized[field]
                if isinstance(token, str) and len(token) > 8:
                    # Show first 4 and last 4 characters
                    sanitized[field] = f"{token[:4]}...{token[-4:]}"
                else:
                    sanitized[field] = "***"

        return sanitized


# Singleton instance for easy access
oauth_manager = OAuthCredentialManager()
