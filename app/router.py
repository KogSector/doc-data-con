"""
Data Connector Service - File Router
Intelligently routes files to appropriate processing services.
"""

import os
import structlog
from typing import Tuple

from app.config import get_settings
from app.models import (
    FileType,
    FileInfo,
    RoutingDecision,
    CODE_EXTENSIONS,
    DOCUMENT_EXTENSIONS,
)

logger = structlog.get_logger()


class FileRouter:
    """Routes files to appropriate processing services based on file type."""

    def __init__(self):
        self.settings = get_settings()

    def detect_file_type(self, file_path: str) -> FileType:
        """
        Detect the type of a file based on its extension.

        Args:
            file_path: Path to the file

        Returns:
            FileType enum indicating whether the file is code, document, or unknown
        """
        _, ext = os.path.splitext(file_path.lower())

        if ext in CODE_EXTENSIONS:
            return FileType.CODE
        elif ext in DOCUMENT_EXTENSIONS:
            return FileType.DOCUMENT
        else:
            return FileType.UNKNOWN

    def get_file_info(self, file_path: str, content: bytes | None = None) -> FileInfo:
        """
        Get detailed information about a file.

        Args:
            file_path: Path to the file
            content: Optional file content for size calculation

        Returns:
            FileInfo object with file details
        """
        name = os.path.basename(file_path)
        _, ext = os.path.splitext(name)
        file_type = self.detect_file_type(file_path)

        size_bytes = len(content) if content else 0

        return FileInfo(
            path=file_path,
            name=name,
            extension=ext,
            size_bytes=size_bytes,
            file_type=file_type,
        )

    def route_file(self, file_path: str) -> RoutingDecision:
        """
        Determine where to route a file based on its type.

        Args:
            file_path: Path to the file

        Returns:
            RoutingDecision with target service information
        """
        file_type = self.detect_file_type(file_path)

        if file_type == FileType.CODE:
            target_service = "unified-processor"
            target_url = self.settings.unified_processor_url
        elif file_type == FileType.DOCUMENT:
            target_service = "unified-processor"
            target_url = self.settings.unified_processor_url
        else:
            # Default unknown files to unified-processor (it can handle all file types)
            target_service = "unified-processor"
            target_url = self.settings.unified_processor_url
            file_type = FileType.DOCUMENT

        logger.debug(
            "Routing decision made",
            file_path=file_path,
            file_type=file_type,
            target_service=target_service,
        )

        return RoutingDecision(
            file_path=file_path,
            file_type=file_type,
            target_service=target_service,
            target_url=target_url,
        )

    def categorize_files(self, file_paths: list[str]) -> Tuple[list[str], list[str], list[str]]:
        """
        Categorize a list of files into code, document, and unknown.

        Args:
            file_paths: List of file paths to categorize

        Returns:
            Tuple of (code_files, document_files, unknown_files)
        """
        code_files = []
        document_files = []
        unknown_files = []

        for path in file_paths:
            file_type = self.detect_file_type(path)

            if file_type == FileType.CODE:
                code_files.append(path)
            elif file_type == FileType.DOCUMENT:
                document_files.append(path)
            else:
                unknown_files.append(path)

        logger.info(
            "Files categorized",
            total=len(file_paths),
            code=len(code_files),
            documents=len(document_files),
            unknown=len(unknown_files),
        )

        return code_files, document_files, unknown_files


# Global router instance
_router: FileRouter | None = None


def get_router() -> FileRouter:
    """Get the global file router instance."""
    global _router
    if _router is None:
        _router = FileRouter()
    return _router
