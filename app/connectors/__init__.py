"""
Data Connector - External Source Connectors.

Handles connectivity to external data sources:
- Git: GitHub, GitLab, Bitbucket
- Cloud Storage: Google Drive, OneDrive, Dropbox, S3
- Docs: Notion
"""

from .gdrive_client import GoogleDriveConnector, GDRIVE_AVAILABLE
from .notion_client import NotionConnector, NOTION_AVAILABLE
from .github_client import GitHubConnector
from .gitlab_client import GitLabConnector
from .bitbucket_client import BitbucketConnector
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
    # Git
    "GitHubConnector",
    "GitLabConnector",
    "BitbucketConnector",
    # Design
    "FigmaConnector",
]
