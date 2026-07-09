"""
Data Connector - External Source Connectors.

Handles connectivity to external data sources:
- Cloud Storage: Google Drive, OneDrive, Dropbox
- Docs: Notion
- Design: Figma
"""

from .gdrive_client import GoogleDriveConnector, GDRIVE_AVAILABLE
from .notion_client import NotionConnector, NOTION_AVAILABLE
from .onedrive_client import OneDriveConnector
from .dropbox_client import DropboxConnector
from .figma_client import FigmaConnector

__all__ = [
    # Cloud Storage
    "GoogleDriveConnector",
    "NotionConnector",
    "OneDriveConnector",
    "DropboxConnector",
    "GDRIVE_AVAILABLE",
    "NOTION_AVAILABLE",
    # Design
    "FigmaConnector",
]
