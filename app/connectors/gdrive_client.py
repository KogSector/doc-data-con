"""
Google Drive Connector - Import documents from Google Drive.
"""

import io
import fnmatch
import structlog
from typing import Optional, Any
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()


# Google API imports - optional
GDRIVE_AVAILABLE = False
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    GDRIVE_AVAILABLE = True
except ImportError:
    logger.warning(
        "Google API not available. Install: pip install google-api-python-client google-auth-oauthlib"
    )


class GoogleDriveConnector(BaseConnector):
    """
    Import documents from Google Drive.

    Features:
    - OAuth2 authentication
    - List files from Drive
    - Download and export documents
    - Support Google Docs, Sheets, Slides (export to supported formats)
    """

    SCOPES = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ]

    # Google Workspace MIME types and export formats
    GOOGLE_EXPORT_FORMATS = {
        "application/vnd.google-apps.document": ("application/pdf", ".pdf", "pdf"),
        "application/vnd.google-apps.spreadsheet": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xlsx",
            "xlsx",
        ),
        "application/vnd.google-apps.presentation": ("application/pdf", ".pdf", "pdf"),
    }

    # Standard file types
    MIME_TO_DOCTYPE = {
        "application/pdf": "pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "text/markdown": "markdown",
        "text/html": "html",
        "text/plain": "text",
        "image/png": "image",
        "image/jpeg": "image",
    }

    def __init__(self, settings: Settings):
        if not GDRIVE_AVAILABLE:
            raise ImportError(
                "Google API not available. Install: pip install google-api-python-client google-auth-oauthlib"
            )

        self.client_id = settings.google_client_id
        self.client_secret = settings.google_client_secret
        self.redirect_uri = (
            settings.google_redirect_uri or "http://localhost:3019/api/v1/gdrive/callback"
        )
        self._service: Any = None
        self._credentials: Any = None

        logger.info("GoogleDriveConnector initialized")

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [self.redirect_uri],
                }
            },
            scopes=self.SCOPES,
        )
        flow.redirect_uri = self.redirect_uri

        auth_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            state=state,
        )
        return auth_url

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """Handle OAuth2 callback and exchange code for tokens."""
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [self.redirect_uri],
                }
            },
            scopes=self.SCOPES,
        )
        flow.redirect_uri = self.redirect_uri
        flow.fetch_token(code=code)

        self._credentials = flow.credentials
        self._service = build("drive", "v3", credentials=self._credentials)

        return {
            "access_token": self._credentials.token,
            "refresh_token": self._credentials.refresh_token,
            "expires_at": self._credentials.expiry.isoformat()
            if self._credentials.expiry
            else None,
        }

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """Refresh an expired access token."""
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            response.raise_for_status()
            tokens = response.json()

        return tokens

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        self._credentials = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            client_id=self.client_id,
            client_secret=self.client_secret,
            token_uri="https://oauth2.googleapis.com/token",
        )
        self._service = build("drive", "v3", credentials=self._credentials)

    async def list_files(
        self,
        folder_id: Optional[str] = None,
        page_size: int = 100,
        page_token: Optional[str] = None,
    ) -> dict:
        """List files from Google Drive."""
        if not self._service:
            raise ValueError("Not authenticated. Call set_credentials first.")

        query = "trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"

        results = (
            self._service.files()
            .list(
                pageSize=page_size,
                pageToken=page_token,
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
            )
            .execute()
        )

        files = []
        for f in results.get("files", []):
            doc_type = self._get_doc_type(f.get("mimeType", ""))
            files.append(
                {
                    "id": f["id"],
                    "name": f["name"],
                    "mime_type": f.get("mimeType"),
                    "size": int(f.get("size", 0)),
                    "modified_at": f.get("modifiedTime"),
                    "doc_type": doc_type if doc_type else None,
                    "supported": doc_type is not None,
                }
            )

        return {
            "files": files,
            "next_page_token": results.get("nextPageToken"),
        }

    async def download_file(self, file_id: str) -> dict:
        """Download a file from Google Drive."""
        if not self._service:
            raise ValueError("Not authenticated. Call set_credentials first.")

        # Get file metadata
        file_meta = (
            self._service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, size",
            )
            .execute()
        )

        mime_type = file_meta.get("mimeType", "")
        filename = file_meta.get("name", "document")

        # Handle Google Workspace files (export)
        if mime_type in self.GOOGLE_EXPORT_FORMATS:
            export_mime, ext, doc_type = self.GOOGLE_EXPORT_FORMATS[mime_type]
            request = self._service.files().export_media(
                fileId=file_id,
                mimeType=export_mime,
            )
            filename = filename + ext
        else:
            # Regular file download
            request = self._service.files().get_media(fileId=file_id)
            doc_type = self._get_doc_type(mime_type)

        # Download content
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        content = buffer.getvalue()

        logger.info(
            "Downloaded file from Google Drive",
            file_id=file_id,
            filename=filename,
            size=len(content),
        )

        return {
            "content": content,
            "filename": filename,
            "doc_type": doc_type,
            "mime_type": mime_type,
            "size": len(content),
            "source": "google_drive",
            "source_id": file_id,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    def _get_doc_type(self, mime_type: str) -> Optional[str]:
        """Get document type from MIME type."""
        if mime_type in self.GOOGLE_EXPORT_FORMATS:
            return self.GOOGLE_EXPORT_FORMATS[mime_type][2]
        return self.MIME_TO_DOCTYPE.get(mime_type)

    async def get_sync_capabilities(self) -> dict:
        """Return what sync methods this provider supports."""
        return {
            "webhooks": False,
            "polling": True,
            "real_time": False,
            "incremental": False,
            "polling_interval": 300,
        }

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from Google Drive for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"], credentials.get("refresh_token"))

        logger.info("Starting Google Drive fetch_source", uri=uri)

        folder_id = uri if uri and uri != "root" and not uri.startswith("oauth://") else None

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
            logger.info("Specific item IDs provided, skipping crawl", count=len(item_ids))
            for i_id in item_ids:
                all_files.append({"id": i_id, "name": f"item_{i_id}", "type": "file", "rel_path": f"item_{i_id}"})
        else:
            # 1. Get all files (recursive crawl)
            async def crawl(current_folder_id=None, current_path=""):
                results = await self.list_files(folder_id=current_folder_id)
                skip_dirs = {".git", "node_modules", "venv", ".venv", "env", ".env", "__pycache__", ".next", "dist", "build"}
                for f in results.get("files", []):
                    rel_path = f"{current_path}/{f['name']}".lstrip("/")
                    if f["mime_type"] == "application/vnd.google-apps.folder":
                        if f["name"] in skip_dirs:
                            logger.info("Skipping ignored directory", dir_name=f["name"])
                            continue
                        await crawl(f["id"], rel_path)
                    else:
                        f["rel_path"] = rel_path
                        all_files.append(f)

            try:
                await crawl(folder_id)
            except Exception as e:
                logger.error("Failed to crawl Google Drive in fetch_source", error=str(e))
                raise

        files_to_download = []
        total_size: int = 0

        for item in all_files:
            path = item["rel_path"]

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
            f"Found {len(files_to_download)} files to download from Google Drive",
            total_size=total_size,
        )

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    file_data = await self.download_file(item["id"])
                    # Ensure the path is preserved
                    file_data["path"] = item["rel_path"]
                    return file_data
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
            "Google Drive fetch_source completed",
            files=len(downloaded_files),
            total_size=total_size,
        )
        return downloaded_files, total_size
