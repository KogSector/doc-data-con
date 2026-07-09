"""
Input validation and sanitization utilities for Data Connector Service.
"""

import re
import urllib.parse
from typing import Any, List, Optional


class ValidationError(Exception):
    """Custom validation error."""

    pass


class InputValidator:
    """Utility class for input validation and sanitization."""

    # Regex patterns
    SAFE_STRING_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")
    REPO_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._/-]+$")
    URL_PATTERN = re.compile(
        r"^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$"
    )
    EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    @staticmethod
    def validate_source_id(source_id: str) -> str:
        """Validate and sanitize source ID."""
        if not source_id or not isinstance(source_id, str):
            raise ValidationError("Source ID is required and must be a string")

        source_id = source_id.strip()

        if len(source_id) > 255:
            raise ValidationError("Source ID must be less than 255 characters")

        if not InputValidator.SAFE_STRING_PATTERN.match(source_id):
            raise ValidationError("Source ID contains invalid characters")

        return source_id

    @staticmethod
    def validate_repository_url(url: str) -> str:
        """Validate and sanitize repository URL."""
        if not url or not isinstance(url, str):
            raise ValidationError("Repository URL is required and must be a string")

        url = url.strip()

        # Basic URL validation
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValidationError("Invalid repository URL format")

        if parsed.scheme not in ["http", "https"]:
            raise ValidationError("Repository URL must use HTTP or HTTPS")

        # Additional validation for common git hosting
        allowed_domains = [
            "github.com",
            "gitlab.com",
            "bitbucket.org",
            "api.github.com",
            "gitlab.com",
            "api.bitbucket.org",
        ]

        if parsed.netloc not in allowed_domains:
            # Allow custom domains but warn
            pass

        return url

    @staticmethod
    def validate_branch_name(branch: Optional[str]) -> Optional[str]:
        """Validate and sanitize branch name."""
        if branch is None:
            return None

        if not isinstance(branch, str):
            raise ValidationError("Branch name must be a string")

        branch = branch.strip()

        if len(branch) > 255:
            raise ValidationError("Branch name must be less than 255 characters")

        # Git branch name validation
        if not re.match(r"^[a-zA-Z0-9._/-]+$", branch):
            raise ValidationError("Branch name contains invalid characters")

        return branch if branch else None

    @staticmethod
    def validate_include_patterns(patterns: List[str]) -> List[str]:
        """Validate and sanitize include patterns."""
        if not patterns:
            return ["**/*"]  # Default pattern

        if not isinstance(patterns, list):
            raise ValidationError("Include patterns must be a list")

        validated_patterns = []
        for pattern in patterns:
            if not isinstance(pattern, str):
                raise ValidationError("Each pattern must be a string")

            pattern = pattern.strip()
            if not pattern:
                continue

            # Basic glob pattern validation
            if len(pattern) > 1000:
                raise ValidationError("Pattern too long")

            validated_patterns.append(pattern)

        return validated_patterns if validated_patterns else ["**/*"]

    @staticmethod
    def validate_exclude_patterns(patterns: List[str]) -> List[str]:
        """Validate and sanitize exclude patterns."""
        if not patterns:
            return []

        return InputValidator.validate_include_patterns(patterns)

    @staticmethod
    def validate_oauth_credentials(credentials: dict[str, Any]) -> dict[str, Any]:
        """Validate OAuth credentials."""
        from app.utils.oauth import oauth_manager

        try:
            return oauth_manager.validate_credentials(credentials)
        except ValueError as e:
            raise ValidationError(str(e))

    @staticmethod
    def sanitize_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
        """Sanitize metadata dictionary."""
        if not isinstance(metadata, dict):
            return {}

        sanitized = {}
        for key, value in metadata.items():
            if not isinstance(key, str) or len(key) > 100:
                continue

            # Convert value to string if it's simple
            if isinstance(value, (str, int, float, bool)):
                sanitized[key] = str(value)[:1000]  # Limit value length
            elif isinstance(value, dict):
                # Recursively sanitize nested dicts
                sanitized[key] = InputValidator.sanitize_metadata(value)
            elif isinstance(value, list):
                # Sanitize list items
                sanitized_list = []
                for item in value[:100]:  # Limit list size
                    if isinstance(item, (str, int, float, bool)):
                        sanitized_list.append(str(item)[:1000])
                sanitized[key] = sanitized_list

        return sanitized

    @staticmethod
    def validate_file_path(file_path: str) -> str:
        """Validate and sanitize file path."""
        if not file_path or not isinstance(file_path, str):
            raise ValidationError("File path is required and must be a string")

        file_path = file_path.strip()

        # Prevent path traversal
        if ".." in file_path or file_path.startswith("/"):
            raise ValidationError("Invalid file path: potential path traversal")

        # Limit length
        if len(file_path) > 1000:
            raise ValidationError("File path too long")

        return file_path

    @staticmethod
    def validate_pagination_params(limit: int, offset: int) -> tuple[int, int]:
        """Validate pagination parameters."""
        if not isinstance(limit, int) or limit < 1 or limit > 1000:
            raise ValidationError("Limit must be between 1 and 1000")

        if not isinstance(offset, int) or offset < 0 or offset > 10000:
            raise ValidationError("Offset must be between 0 and 10000")

        return limit, offset
