"""
Base Connector - Common functionality for all connectors
"""

import hashlib
import structlog
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from app.config import Settings

logger = structlog.get_logger()


class BaseConnector(ABC):
    """Base class for all connectors with common OAuth and sync functionality."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        logger.info(f"{self.__class__.__name__} initialized")

    @abstractmethod
    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        pass

    @abstractmethod
    async def exchange_code_for_token(
        self, code: str, redirect_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """Exchange authorization code for access token."""
        pass

    @abstractmethod
    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh an expired access token."""
        pass

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        self._access_token = access_token
        self._refresh_token = refresh_token

    @abstractmethod
    async def get_sync_capabilities(self) -> Dict[str, Any]:
        """Return what sync methods this provider supports."""
        pass

    def _hash_content(self, content: bytes) -> str:
        """Generate SHA-256 hash of content."""
        return hashlib.sha256(content).hexdigest()

    def _format_timestamp(self, dt: Optional[datetime] = None) -> Optional[str]:
        """Format datetime to ISO string."""
        if not dt:
            return None
        return dt.isoformat()

    def _create_file_metadata(
        self, file_path: str, content: bytes, file_type: str = "unknown"
    ) -> Dict[str, Any]:
        """Create standard file metadata."""
        return {
            "path": file_path,
            "size": len(content),
            "hash": self._hash_content(content),
            "type": file_type,
            "last_modified": self._format_timestamp(datetime.now(timezone.utc)),
        }





class CloudStorageMixin:
    """Common functionality for cloud storage providers."""

    async def sync_folder(
        self, folder_id: str, include_patterns: list = None, exclude_patterns: list = None
    ) -> list[Dict[str, Any]]:
        """Sync folder - to be implemented by specific provider."""
        raise NotImplementedError
