"""
OneDrive/SharePoint Connector - Import files from Microsoft Graph API.

Implements OAuth2 authentication, file/folder access, and delta sync.
"""

import hashlib
import fnmatch
import structlog
from typing import Optional, Any
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()


class OneDriveConnector(BaseConnector):
    """
    Import files from OneDrive and SharePoint using Microsoft Graph API.

    Features:
    - OAuth2 authentication with Microsoft identity platform
    - List files and folders
    - Download files with metadata
    - Delta sync support for incremental updates
    - SharePoint site access
    """

    SCOPES = ["Files.Read", "Files.ReadWrite", "offline_access", "User.Read"]

    def __init__(self, settings: Settings):
        self.client_id = settings.microsoft_client_id
        self.client_secret = settings.microsoft_client_secret
        self.tenant_id = settings.microsoft_tenant_id
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

        # Construct URLs based on tenant
        self.auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/authorize"
        self.token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        self.api_url = "https://graph.microsoft.com/v1.0"

        logger.info("OneDriveConnector initialized", tenant=self.tenant_id)

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "scope": " ".join(self.SCOPES),
            "response_mode": "query",
        }

        if state:
            params["state"] = state
        if redirect_uri:
            params["redirect_uri"] = redirect_uri

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.auth_url}?{query}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """Handle OAuth2 callback and exchange code for tokens."""
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            tokens = response.json()

        if "error" in tokens:
            raise ValueError(
                f"Microsoft OAuth error: {tokens.get('error_description', tokens['error'])}"
            )

        self._access_token = tokens["access_token"]
        self._refresh_token = tokens.get("refresh_token")

        logger.info("OneDrive OAuth completed successfully")

        return {
            "access_token": tokens["access_token"],
            "refresh_token": tokens.get("refresh_token"),
            "token_type": tokens.get("token_type", "Bearer"),
            "expires_in": tokens.get("expires_in"),
        }

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """Refresh an expired access token."""
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                    "scope": " ".join(self.SCOPES),
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            tokens = response.json()

        self._access_token = tokens["access_token"]
        self._refresh_token = tokens.get("refresh_token")

        return tokens

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        self._access_token = access_token
        self._refresh_token = refresh_token

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        """Make authenticated API request."""
        import httpx

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"

        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, **kwargs)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                try:
                    error_data = e.response.json()
                    err_msg = error_data.get("error", {}).get("message", e.response.text)
                except Exception:
                    err_msg = e.response.text

                if "SPO license" in err_msg or "Tenant does not have" in err_msg:
                    err_msg = "Your Microsoft account does not have a OneDrive or SharePoint license provisioned. Please use an account with an active Microsoft 365 subscription."

                from fastapi import HTTPException

                raise HTTPException(status_code=400, detail=f"Microsoft Graph API Error: {err_msg}")
            if response.content:
                return response.json()
            return {}

    async def get_user_info(self) -> dict:
        """Get authenticated user information."""
        user = await self._request("GET", "/me")
        return {
            "id": user.get("id"),
            "displayName": user.get("displayName"),
            "mail": user.get("mail"),
            "userPrincipalName": user.get("userPrincipalName"),
        }

    async def list_drive_items(
        self,
        path: str = "",
        drive_id: Optional[str] = None,
        per_page: int = 100,
        folder_id: Optional[str] = None,
    ) -> list[dict]:
        """
        List files and folders in OneDrive.

        Args:
            path: Path within the drive (empty for root)
            drive_id: Specific drive ID (None for default drive)
            per_page: Results per page

        Returns:
            List of file/folder information
        """
        if folder_id:
            if drive_id:
                endpoint = f"/drives/{drive_id}/items/{folder_id}/children"
            else:
                endpoint = f"/me/drive/items/{folder_id}/children"
        elif drive_id:
            if path:
                endpoint = f"/drives/{drive_id}/root:/{path}:/children"
            else:
                endpoint = f"/drives/{drive_id}/root/children"
        else:
            if path:
                endpoint = f"/me/drive/root:/{path}:/children"
            else:
                endpoint = "/me/drive/root/children"

        data = await self._request("GET", f"{endpoint}?$top={per_page}")

        items = []
        for item in data.get("value", []):
            items.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "path": item.get("parentReference", {}).get("path", "") + "/" + item["name"],
                    "type": "folder" if "folder" in item else "file",
                    "size": item.get("size"),
                    "mimeType": item.get("file", {}).get("mimeType"),
                    "webUrl": item.get("webUrl"),
                    "createdDateTime": item.get("createdDateTime"),
                    "lastModifiedDateTime": item.get("lastModifiedDateTime"),
                    "cTag": item.get("cTag"),  # Content tag for change detection
                    "eTag": item.get("eTag"),  # Entity tag
                }
            )

        logger.info("Listed OneDrive items", path=path, count=len(items))
        return items

    async def download_file(
        self,
        item_id: str,
        drive_id: Optional[str] = None,
    ) -> dict:
        """
        Download file content via item ID.

        Args:
            item_id: OneDrive item ID
            drive_id: Specific drive ID

        Returns:
            File content and metadata
        """
        import httpx

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        # Get item metadata first
        if drive_id:
            endpoint = f"/drives/{drive_id}/items/{item_id}"
        else:
            endpoint = f"/me/drive/items/{item_id}"

        metadata = await self._request("GET", endpoint)

        # Download content
        download_url = metadata.get("@microsoft.graph.downloadUrl")
        if not download_url:
            raise ValueError("No download URL available for this item")

        async with httpx.AsyncClient() as client:
            response = await client.get(download_url)
            response.raise_for_status()
            content = response.content

        content_hash = hashlib.sha256(content).hexdigest()
        filename = metadata.get("name", "unknown")
        extension = filename.rsplit(".", 1)[-1] if "." in filename else ""

        logger.info(
            "Downloaded file from OneDrive",
            item_id=item_id,
            filename=filename,
            size=len(content),
        )

        return {
            "content": content,
            "filename": filename,
            "path": metadata.get("parentReference", {}).get("path", "") + "/" + filename,
            "id": item_id,
            "size": len(content),
            "content_hash": content_hash,
            "extension": extension,
            "mimeType": metadata.get("file", {}).get("mimeType"),
            "source": "onedrive",
            "source_id": f"onedrive:{item_id}",
            "lastModifiedDateTime": metadata.get("lastModifiedDateTime"),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    async def get_delta(
        self,
        drive_id: Optional[str] = None,
        delta_link: Optional[str] = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Get delta changes for incremental sync.

        Args:
            drive_id: Specific drive ID
            delta_link: Previous delta link to get changes since

        Returns:
            Tuple of (changed items, new delta link)
        """
        changes = []
        while True:
            if delta_link:
                # Use delta link directly
                import httpx

                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        delta_link,
                        headers={"Authorization": f"Bearer {self._access_token}"},
                    )
                    response.raise_for_status()
                    data = response.json()
            else:
                if drive_id:
                    endpoint = f"/drives/{drive_id}/root/delta"
                else:
                    endpoint = "/me/drive/root/delta"
                data = await self._request("GET", endpoint)

            # Process this page of changes
            for item in data.get("value", []):
                change_type = "deleted" if item.get("deleted") else "modified"
                changes.append(
                    {
                        "id": item["id"],
                        "name": item.get("name"),
                        "change_type": change_type,
                        "path": item.get("parentReference", {}).get("path", "")
                        + "/"
                        + item.get("name", ""),
                        "type": "folder" if "folder" in item else "file",
                        "lastModifiedDateTime": item.get("lastModifiedDateTime"),
                    }
                )

            # Check for next page
            next_link = data.get("@odata.nextLink")
            if next_link:
                delta_link = next_link
                continue

            # If no next page, we found the final delta link
            new_delta_link = data.get("@odata.deltaLink")
            break

        logger.info(
            "Got OneDrive delta",
            changes_count=len(changes),
            has_delta_link=bool(new_delta_link),
        )

        return changes, new_delta_link

    # SharePoint access

    async def list_sharepoint_sites(self, search: Optional[str] = None) -> list[dict]:
        """List accessible SharePoint sites."""
        endpoint = "/sites"
        params = {}
        if search:
            endpoint = f"/sites?search={search}"

        data = await self._request("GET", endpoint, params=params)

        sites = []
        for site in data.get("value", []):
            sites.append(
                {
                    "id": site["id"],
                    "name": site.get("name"),
                    "displayName": site.get("displayName"),
                    "webUrl": site.get("webUrl"),
                }
            )

        return sites

    async def list_sharepoint_drives(self, site_id: str) -> list[dict]:
        """List drives in a SharePoint site."""
        data = await self._request("GET", f"/sites/{site_id}/drives")

        drives = []
        for drive in data.get("value", []):
            drives.append(
                {
                    "id": drive["id"],
                    "name": drive.get("name"),
                    "driveType": drive.get("driveType"),
                    "webUrl": drive.get("webUrl"),
                }
            )

        return drives

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
            "delta_sync": True,
            "polling_interval": 300,
        }

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from OneDrive for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"], credentials.get("refresh_token"))

        logger.info("Starting OneDrive fetch_source", uri=uri)

        drive_id = uri if uri and uri != "root" and not uri.startswith("oauth://") else None

        # Check if specific items were selected
        source_metadata = kwargs.get("source_metadata", {})
        item_ids = (
            credentials.get("item_ids") or 
            source_metadata.get("item_ids") or 
            source_metadata.get("metadata", {}).get("item_ids") or 
            []
        )

        all_files = []
        if item_ids:
            # Skip recursive crawl, fetch only specific items
            logger.info("Specific item IDs provided, skipping crawl", count=len(item_ids))
            for i_id in item_ids:
                # We can't know the exact metadata without querying, but download_file fetches it anyway
                all_files.append({"id": i_id, "name": f"item_{i_id}", "type": "file", "path": ""})
        else:
            # 1. Get all files (recursive crawl)
            async def crawl(rel_path=""):
                items = await self.list_drive_items(path=rel_path, drive_id=drive_id)
                skip_dirs = {".git", "node_modules", "venv", ".venv", "env", ".env", "__pycache__", ".next", "dist", "build"}
                for item in items:
                    if item["type"] == "file":
                        all_files.append(item)
                    elif item["type"] == "folder":
                        if item["name"] in skip_dirs:
                            logger.info("Skipping ignored directory", dir_name=item["name"])
                            continue
                        new_rel_path = f"{rel_path}/{item['name']}".lstrip("/")
                        await crawl(new_rel_path)

            try:
                await crawl()
            except Exception as e:
                logger.error("Failed to crawl OneDrive in fetch_source", error=str(e))
                raise

        files_to_download = []
        total_size = 0

        for item in all_files:
            # Only apply pattern matching if we are not given specific item_ids
            if not item_ids:
                path = item["name"]
                if "root:/" in item.get("path", ""):
                    path = item["path"].split("root:/")[-1].lstrip("/")

                def match_pattern(path_str: str, pattern: str) -> bool:
                    if pattern == "**/*" or pattern == "*":
                        return True
                    pat = pattern.replace("**/*", "*").replace("**", "*")
                    return fnmatch.fnmatch(path_str, pat)

                included = any(match_pattern(path, p) for p in include_patterns)
                if not included:
                    continue

                excluded = any(match_pattern(path, p) for p in exclude_patterns)
                if excluded:
                    continue

            files_to_download.append(item)
            size_val = item.get("size")
            item_size: int = int(size_val) if size_val else 0
            total_size += item_size

        logger.info(
            f"Found {len(files_to_download)} files to download from OneDrive", total_size=total_size
        )

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    return await self.download_file(item["id"], drive_id=drive_id)
                except Exception as e:
                    logger.warning(
                        "Failed to download file during fetch_source",
                        path=item["name"],
                        error=str(e),
                    )
                    return None

        tasks = [_download_file_concurrently(item) for item in files_to_download]
        results = await asyncio.gather(*tasks)

        downloaded_files = [res for res in results if res is not None]

        logger.info(
            "OneDrive fetch_source completed", files=len(downloaded_files), total_size=total_size
        )
        return downloaded_files, total_size
