"""
Dropbox Connector - Import files from Dropbox.

Implements OAuth2 authentication, file/folder access, and cursor-based delta sync.
"""

import hashlib
import fnmatch
import structlog
from typing import Optional, Any
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()


class DropboxConnector(BaseConnector):
    """
    Import files from Dropbox.

    Features:
    - OAuth2 authentication
    - List files and folders
    - Download files with metadata
    - Cursor-based delta sync for incremental updates
    """

    SCOPES = ["files.metadata.read", "files.content.read"]
    AUTH_URL = "https://www.dropbox.com/oauth2/authorize"
    TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
    API_URL = "https://api.dropboxapi.com/2"
    CONTENT_URL = "https://content.dropboxapi.com/2"

    def __init__(self, settings: Settings):
        BaseConnector.__init__(self, settings)
        self.client_id = settings.dropbox_client_id
        self.client_secret = settings.dropbox_client_secret

        logger.info("DropboxConnector initialized")

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "token_access_type": "offline",  # Get refresh token
        }

        if state:
            params["state"] = state
        if redirect_uri:
            params["redirect_uri"] = redirect_uri

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.AUTH_URL}?{query}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """Handle OAuth2 callback and exchange code for tokens."""
        import httpx

        data = {
            "code": code,
            "grant_type": "authorization_code",
        }
        if redirect_uri:
            data["redirect_uri"] = redirect_uri

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data=data,
                auth=(self.client_id, self.client_secret),
            )
            response.raise_for_status()
            tokens = response.json()

        if "error" in tokens:
            raise ValueError(
                f"Dropbox OAuth error: {tokens.get('error_description', tokens['error'])}"
            )

        self._access_token = tokens["access_token"]
        self._refresh_token = tokens.get("refresh_token")

        logger.info("Dropbox OAuth completed successfully")

        return {
            "access_token": tokens["access_token"],
            "refresh_token": tokens.get("refresh_token"),
            "token_type": tokens.get("token_type", "bearer"),
            "expires_in": tokens.get("expires_in"),
            "uid": tokens.get("uid"),
            "account_id": tokens.get("account_id"),
        }

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """Refresh an expired access token."""
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
                auth=(self.client_id, self.client_secret),
            )
            response.raise_for_status()
            tokens = response.json()

        self._access_token = tokens["access_token"]

        return tokens

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        self._access_token = access_token
        self._refresh_token = refresh_token

    async def _request(
        self, endpoint: str, data: Optional[dict] = None, **kwargs
    ) -> dict[str, Any]:
        """Make authenticated API request."""
        import httpx

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        url = f"{self.API_URL}/{endpoint.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"
        headers["Content-Type"] = "application/json"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data or {}, headers=headers, **kwargs)
            response.raise_for_status()
            if response.content:
                return response.json()
            return {}

    async def get_account_info(self) -> dict:
        """Get authenticated user account info."""
        account = await self._request("/users/get_current_account")
        return {
            "account_id": account.get("account_id"),
            "name": account.get("name", {}).get("display_name"),
            "email": account.get("email"),
            "email_verified": account.get("email_verified"),
        }

    async def list_folder(
        self,
        path: str = "",
        recursive: bool = False,
        limit: int = 2000,
    ) -> list[dict]:
        """
        List files and folders in a path.

        Args:
            path: Folder path (empty string for root)
            recursive: Include all subdirectories
            limit: Maximum entries to return

        Returns:
            List of file/folder information
        """
        data = {
            "path": path if path else "",
            "recursive": recursive,
            "limit": limit,
            "include_deleted": False,
        }

        result = await self._request("/files/list_folder", data)

        items = []
        for entry in result.get("entries", []):
            items.append(self._parse_entry(entry))

        # Handle pagination
        cursor = result.get("cursor")
        has_more = result.get("has_more", False)

        while has_more:
            continue_result = await self._request("/files/list_folder/continue", {"cursor": cursor})
            for entry in continue_result.get("entries", []):
                items.append(self._parse_entry(entry))

            cursor = continue_result.get("cursor")
            has_more = continue_result.get("has_more", False)

        logger.info("Listed Dropbox folder", path=path, count=len(items))
        return items

    def _parse_entry(self, entry: dict) -> dict:
        """Parse a Dropbox entry to standardized format."""
        return {
            "id": entry.get("id"),
            "name": entry.get("name"),
            "path": entry.get("path_display"),
            "path_lower": entry.get("path_lower"),
            "type": "folder" if entry.get(".tag") == "folder" else "file",
            "size": entry.get("size"),
            "content_hash": entry.get("content_hash"),
            "server_modified": entry.get("server_modified"),
            "client_modified": entry.get("client_modified"),
            "rev": entry.get("rev"),  # Revision for change detection
        }

    async def download_file(self, path: str) -> dict:
        """
        Download file content with metadata.

        Args:
            path: File path in Dropbox

        Returns:
            File content and metadata
        """
        import httpx
        import json

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        url = f"{self.CONTENT_URL}/files/download"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Dropbox-API-Arg": json.dumps({"path": path}),
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers)
            response.raise_for_status()
            content = response.content

            # Metadata is in response header
            metadata_str = response.headers.get("dropbox-api-result", "{}")
            metadata = json.loads(metadata_str)

        content_hash = hashlib.sha256(content).hexdigest()
        filename = metadata.get("name", path.rsplit("/", 1)[-1])
        extension = filename.rsplit(".", 1)[-1] if "." in filename else ""

        logger.info(
            "Downloaded file from Dropbox",
            path=path,
            size=len(content),
        )

        return {
            "content": content,
            "filename": filename,
            "path": path,
            "id": metadata.get("id"),
            "size": len(content),
            "content_hash": content_hash,
            "extension": extension,
            "rev": metadata.get("rev"),
            "source": "dropbox",
            "source_id": f"dropbox:{metadata.get('id')}",
            "server_modified": metadata.get("server_modified"),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    async def get_latest_cursor(self, path: str = "", recursive: bool = True) -> str:
        """
        Get cursor for delta sync.

        Args:
            path: Root path to track
            recursive: Include subdirectories

        Returns:
            Cursor string for polling changes
        """
        result = await self._request(
            "/files/list_folder/get_latest_cursor",
            {"path": path if path else "", "recursive": recursive},
        )
        return str(result.get("cursor") or "")

    async def get_changes(self, cursor: str) -> tuple[list[dict], str]:
        """
        Get changes since cursor for incremental sync.

        Args:
            cursor: Previous cursor from get_latest_cursor or get_changes

        Returns:
            Tuple of (changed entries, new cursor)
        """
        result = await self._request("/files/list_folder/continue", {"cursor": cursor})

        changes = []
        for entry in result.get("entries", []):
            entry_parsed = self._parse_entry(entry)

            # Detect change type
            if entry.get(".tag") == "deleted":
                entry_parsed["change_type"] = "deleted"
            else:
                entry_parsed["change_type"] = "modified"

            changes.append(entry_parsed)

        new_cursor = result.get("cursor")

        # Handle pagination
        has_more = result.get("has_more", False)
        while has_more:
            continue_result = await self._request(
                "/files/list_folder/continue", {"cursor": new_cursor}
            )
            for entry in continue_result.get("entries", []):
                entry_parsed = self._parse_entry(entry)
                if entry.get(".tag") == "deleted":
                    entry_parsed["change_type"] = "deleted"
                else:
                    entry_parsed["change_type"] = "modified"
                changes.append(entry_parsed)

            new_cursor = continue_result.get("cursor")
            has_more = continue_result.get("has_more", False)

        logger.info("Got Dropbox changes", count=len(changes))
        return changes, str(new_cursor or "")

    # =========================================================================
    # Sync Provider Interface Implementation
    # =========================================================================

    async def get_sync_capabilities(self) -> dict:
        """Return what sync methods this provider supports."""
        return {
            "webhooks": True,
            "polling": True,
            "real_time": False,
            "incremental": True,
            "cursor_sync": True,
            "polling_interval": 300,
        }

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from Dropbox for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"], credentials.get("refresh_token"))

        logger.info("Starting Dropbox fetch_source", uri=uri)

        path = uri if uri and not uri.startswith("oauth://") else ""

        # Check if specific items were selected
        source_metadata = kwargs.get("source_metadata", {})
        item_ids = (
            credentials.get("item_ids") or 
            source_metadata.get("item_ids") or 
            source_metadata.get("metadata", {}).get("item_ids") or 
            []
        )

        all_entries = []
        if item_ids:
            logger.info("Specific item IDs provided, skipping crawl", count=len(item_ids))
            for i_id in item_ids:
                all_entries.append({"id": i_id, "name": f"item_{i_id}", "type": "file", "path": f"item_{i_id}"})
        else:
            # 1. Get all files (recursive)
            try:
                all_entries = await self.list_folder(path=path, recursive=True)
            except Exception as e:
                logger.error("Failed to list folder in Dropbox fetch_source", error=str(e))
                raise

        files_to_download = []
        total_size: int = 0

        for item in all_entries:
            if item["type"] != "file":
                continue

            # item["path"] is path_display from parse_entry
            rel_path = item["path"].lstrip("/")

            def match_pattern(path_str: str, pattern: str) -> bool:
                if pattern == "**/*" or pattern == "*":
                    return True
                pat = pattern.replace("**/*", "*").replace("**", "*")
                return fnmatch.fnmatch(path_str, pat)

            included = any(match_pattern(rel_path, p) for p in include_patterns)
            if not included:
                continue

            excluded = any(match_pattern(rel_path, p) for p in exclude_patterns)
            if excluded:
                continue

            files_to_download.append(item)
            item_size: int = int(item.get("size") or 0)
            total_size += item_size

        logger.info(
            f"Found {len(files_to_download)} files to download from Dropbox", total_size=total_size
        )

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    return await self.download_file(item["path"])
                except Exception as e:
                    logger.warning(
                        "Failed to download file during fetch_source",
                        path=item["path"],
                        error=str(e),
                    )
                    return None

        tasks = [_download_file_concurrently(item) for item in files_to_download]
        results = await asyncio.gather(*tasks)

        downloaded_files = [res for res in results if res is not None]

        logger.info(
            "Dropbox fetch_source completed", files=len(downloaded_files), total_size=total_size
        )
        return downloaded_files, total_size
