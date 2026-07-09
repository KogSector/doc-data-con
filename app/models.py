"""
Data Connector Service - Data Models
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field


class SourceType(str, Enum):
    """Supported source types."""

    GIT = "git"
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    LOCAL_FILES = "local_files"
    S3 = "s3"
    GDRIVE = "gdrive"
    DROPBOX = "dropbox"
    ONEDRIVE = "onedrive"
    URL = "url"
    UPLOAD = "upload"
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    DRATA = "drata"
    VANTA = "vanta"
    NOTION = "notion"
    CONFLUENCE = "confluence"
    SLACK = "slack"
    CUSTOM = "custom"
    SQL_DATABASE = "sql_database"
    NOSQL_DATABASE = "nosql_database"


class FileType(str, Enum):
    """File type categories for routing."""

    CODE = "code"
    DOCUMENT = "document"
    UNKNOWN = "unknown"


class JobStatus(str, Enum):
    """Processing job status."""

    PENDING = "pending"
    PROCESSING = "processing"
    ROUTING = "routing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# File extension mappings for routing
CODE_EXTENSIONS = {
    # Python
    ".py",
    ".pyw",
    ".pyi",
    ".pyx",
    # JavaScript/TypeScript
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".mjs",
    ".cjs",
    # Web & Frameworks
    ".html",
    ".htm",
    ".xhtml",
    ".css",
    ".scss",
    ".less",
    ".vue",
    ".svelte",
    # Rust
    ".rs",
    # Go
    ".go",
    # Java/Kotlin
    ".java",
    ".kt",
    ".kts",
    # C/C++
    ".c",
    ".h",
    ".cpp",
    ".hpp",
    ".cc",
    ".hh",
    ".cxx",
    # C#
    ".cs",
    # Ruby
    ".rb",
    ".rake",
    # PHP
    ".php",
    # Swift
    ".swift",
    # Scala
    ".scala",
    # Shell
    ".sh",
    ".bash",
    ".zsh",
    ".fish",
    # PowerShell
    ".ps1",
    ".psm1",
    # Lua
    ".lua",
    # R
    ".r",
    ".R",
    # Julia
    ".jl",
    # Dart
    ".dart",
    # Elixir
    ".ex",
    ".exs",
    # Haskell
    ".hs",
    # Clojure
    ".clj",
    ".cljs",
    ".cljc",
    # SQL
    ".sql",
    # Config as code & Data
    ".tf",
    ".hcl",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".xml",
}

DOCUMENT_EXTENSIONS = {
    # Documents
    ".pdf",
    ".doc",
    ".docx",
    ".odt",
    ".rtf",
    # Markdown/Text
    ".md",
    ".markdown",
    ".txt",
    ".rst",
    ".adoc",
    # Presentations
    ".ppt",
    ".pptx",
    ".odp",
    # Spreadsheets
    ".xls",
    ".xlsx",
    ".csv",
    ".ods",
    # eBooks
    ".epub",
    ".mobi",
}


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    type: SourceType
    name: str = Field(..., min_length=1, max_length=255)
    uri: str = Field(..., min_length=1)
    credentials: Optional[dict[str, Any]] = None
    branch: Optional[str] = None
    include_patterns: list[str] = Field(default_factory=lambda: ["**/*"])
    exclude_patterns: list[str] = Field(default_factory=list)
    # File extensions to process (e.g., [".py", ".rs", ".js"])
    # If empty, uses default CODE_EXTENSIONS for code and DOCUMENT_EXTENSIONS for docs
    file_extensions: Optional[list[str]] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceResponse(BaseModel):
    """Response model for source operations."""

    id: str
    type: SourceType
    name: str
    uri: str
    status: str = "active"
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
    syncStarted: Optional[bool] = None


class FileInfo(BaseModel):
    """Information about a file to be processed."""

    path: str
    name: str
    extension: str
    size_bytes: int
    file_type: FileType
    content_hash: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProcessingJob(BaseModel):
    """A processing job for ingesting sources."""

    id: str
    source_id: str
    source_type: SourceType
    status: JobStatus
    total_files: int = 0
    processed_files: int = 0
    code_files: int = 0
    document_files: int = 0
    errors: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestRequest(BaseModel):
    """Request to ingest a source."""

    source_id: str
    force_reprocess: bool = False
    include_patterns: Optional[list[str]] = None
    exclude_patterns: Optional[list[str]] = None
    # File extensions to process - passed to unified-processor
    file_extensions: Optional[list[str]] = None


class RoutingDecision(BaseModel):
    """Decision on where to route a file."""

    file_path: str
    file_type: FileType
    target_service: str
    target_url: str
