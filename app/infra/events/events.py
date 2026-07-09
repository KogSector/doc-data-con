"""
Event Definitions for ConFuse Platform

Python event classes that correspond to the Protobuf definitions.
These are used for serialization/deserialization with Kafka.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
import uuid

from .topics import Topics


# =============================================================================
# Common Types
# =============================================================================


class FileType(str, Enum):
    """File type classification"""

    UNKNOWN = "unknown"
    CODE = "code"
    DOCUMENT = "document"


class SourceType(str, Enum):
    """Source types for ingestion"""

    UNKNOWN = "unknown"
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    LOCAL = "local"
    GOOGLE_DRIVE = "google_drive"
    NOTION = "notion"
    FILE_UPLOAD = "file_upload"
    DROPBOX = "dropbox"
    ONEDRIVE = "onedrive"
    WEB = "web"
    URL = "url"


class EventHeaders(BaseModel):
    """Event headers included in all events"""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_service: str
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None

    @classmethod
    def create(cls, source_service: str, event_type: str) -> "EventHeaders":
        return cls(
            source_service=source_service,
            event_type=event_type,
        )


class EventMetadata(BaseModel):
    """Event metadata for processing context"""

    retry_count: int = 0
    original_event_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None


# =============================================================================
# Embedding Events
# =============================================================================


class EmbeddingGeneratedEvent(BaseModel):
    """
    Event published when embeddings have been generated
    Topic: embedding.generated
    """

    headers: EventHeaders
    metadata: EventMetadata = Field(default_factory=EventMetadata)
    file_id: str
    source_id: str
    chunk_ids: List[str]
    embedding_model: str
    embedding_dimension: int
    total_chunks: int
    vector_storage_location: Optional[str] = None
    processing_time_ms: int

    @staticmethod
    def topic() -> str:
        return Topics.EMBEDDING_GENERATED


# =============================================================================
# Source Sync Events
# =============================================================================



# =============================================================================
# Event-Driven Pipeline Events (Refactored Architecture)
# =============================================================================


class RepoIngestRequestedEvent(BaseModel):
    """
    Event published when a repository ingestion is requested
    Topic: repo.events
    """

    event_type: str = "REPO_INGEST_REQUESTED"
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    payload: "RepoIngestRequestedPayload"

    @staticmethod
    def topic() -> str:
        return Topics.REPO_EVENTS


class RepoIngestRequestedPayload(BaseModel):
    """Payload for REPO_INGEST_REQUESTED event"""

    repo_id: str
    url: str
    branch: str
    provider: str  # github, gitlab, bitbucket
    commit_id: str
    credential_ref: str  # JWT token for credential exchange
    user_id: str
    organization_id: Optional[str] = None


class RepoUpdatedEvent(BaseModel):
    """
    Event published when a repository is updated (webhook)
    Topic: repo.events
    """

    event_type: str = "REPO_UPDATED"
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    payload: "RepoUpdatedPayload"

    @staticmethod
    def topic() -> str:
        return Topics.REPO_EVENTS


class RepoUpdatedPayload(BaseModel):
    """Payload for REPO_UPDATED event"""

    repo_id: str
    url: str
    branch: str
    provider: str
    old_commit: str
    new_commit: str
    credential_ref: str
    update_type: str  # push, force_push, branch_update


class RepoIngestFailedEvent(BaseModel):
    """
    Event published when repository ingestion fails
    Topic: repo.events
    """

    event_type: str = "REPO_INGEST_FAILED"
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: str
    payload: "RepoIngestFailedPayload"

    @staticmethod
    def topic() -> str:
        return Topics.REPO_EVENTS


class RepoIngestFailedPayload(BaseModel):
    """Payload for REPO_INGEST_FAILED event"""

    repo_id: str
    error_code: str  # CLONE_FAILED, AUTH_FAILED, INVALID_REPO, PROCESSING_FAILED, TIMEOUT
    error_message: str
    retry_count: int = 0
    fatal: bool = False


class RepoIngestCompletedEvent(BaseModel):
    """
    Event published when repository ingestion completes successfully
    Topic: repo.events
    """

    event_type: str = "REPO_INGEST_COMPLETED"
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: str
    payload: "RepoIngestCompletedPayload"

    @staticmethod
    def topic() -> str:
        return Topics.REPO_EVENTS


class RepoIngestCompletedPayload(BaseModel):
    """Payload for REPO_INGEST_COMPLETED event"""

    repo_id: str
    commit_id: str
    stats: "RepoIngestStats"


class RepoIngestStats(BaseModel):
    """Statistics for repository ingestion"""

    files_processed: int
    chunks_created: int
    processing_duration_ms: int
    repository_size_bytes: int


class StreamedFileEvent(BaseModel):
    """
    Streamed file from repository sync.
    """

    event_type: str = "STREAMED_FILE"
    repo_id: str
    repo_name: str
    file_path: str
    content: str
    url: str
    user_id: Optional[str] = None
    is_deleted: bool = False

    @staticmethod
    def topic() -> str:
        return Topics.REPO_EVENTS
