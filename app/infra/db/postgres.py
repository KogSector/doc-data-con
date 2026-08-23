"""
Data Connector Service - PostgreSQL Database Connection for Documents and Jobs Management
"""

import structlog
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, Integer, Text, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.config import get_settings
import uuid

logger = structlog.get_logger()


# Base class for all models
class Base(DeclarativeBase):
    pass


# Database Models
class Document(Base):
    """Connected Documents (PDF, Word, Markdown, Google Drive, Notion, etc.)."""

    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    name = Column(String(255), nullable=False)
    doc_type = Column(String(50), nullable=False)  # pdf, docx, txt, md, etc.
    source = Column(String(50), nullable=False)  # upload, google_drive, notion, url, etc.
    uri = Column(String(500), nullable=True)
    size = Column(String(50), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    tags = Column(JSONB, nullable=True)
    status = Column(String(50), default="active")  # active, indexing, error
    document_metadata = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id = Column(UUID(as_uuid=True), nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    status = Column(String(50), nullable=False)  # pending, running, completed, failed
    source_type = Column(String(50), nullable=False)
    job_type = Column(String(50), nullable=False)  # sync, process, index
    progress = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# Global database engine and session
_engine = None
_session_factory = None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((ConnectionError, OSError)),
    before_sleep=lambda retry_state: logger.warning(
        "PostgreSQL connection attempt failed, retrying",
        attempt=retry_state.attempt_number,
        error=str(retry_state.outcome.exception()),
    ),
)
async def _connect_with_retry(database_url: str):
    """Connect to PostgreSQL with retry logic."""
    global _engine, _session_factory

    logger.info("Connecting to PostgreSQL", url=database_url)

    # Configure async engine — only use SSL for cloud (Neon) PostgreSQL
    connect_args = {}
    if "neon.tech" in database_url or "sslmode=" in database_url:
        connect_args["ssl"] = "require"

    _engine = create_async_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=False,
        connect_args=connect_args,
    )

    _session_factory = async_sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False
    )

    # Test connection
    async with _engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    logger.info("Connected to PostgreSQL successfully")


async def init_postgresql() -> None:
    """Initialize PostgreSQL connection and create tables."""
    try:
        settings = get_settings()
        await _connect_with_retry(settings.database_url)

        # Drop legacy sources table and create tables
        async with _engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS sources CASCADE;"))
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)

        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.error("Failed to initialize PostgreSQL", error=str(e))
        raise


async def close_postgresql() -> None:
    """Close PostgreSQL connection gracefully."""
    global _engine

    if _engine:
        try:
            await _engine.dispose()
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.warning("Error closing PostgreSQL connection", error=str(e))
        finally:
            _engine = None
            _session_factory = None


def get_session() -> AsyncSession:
    """Get a database session."""
    if not _session_factory:
        raise RuntimeError("PostgreSQL not initialized. Call init_postgresql() first.")
    return _session_factory()


async def health_check() -> dict:
    """Perform PostgreSQL health check."""
    if not _engine:
        return {"status": "uninitialized", "message": "PostgreSQL not initialized"}

    try:
        async with _engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        pool_info = {
            "pool_size": _engine.pool.size(),
            "checked_in": _engine.pool.checkedin(),
            "checked_out": _engine.pool.checkedout(),
        }

        return {"status": "healthy", "message": "PostgreSQL is healthy", "pool": pool_info}

    except Exception as e:
        logger.error("PostgreSQL health check failed", error=str(e))
        return {"status": "unhealthy", "message": f"PostgreSQL health check failed: {str(e)}"}


# Database Operations
class DocumentManager:
    """Manages document operations in PostgreSQL."""

    @staticmethod
    async def create_document(doc_data: dict) -> uuid.UUID:
        """Create a new document."""
        async with get_session() as session:
            doc = Document(**doc_data)
            session.add(doc)
            await session.commit()
            await session.refresh(doc)
            return doc.id

    @staticmethod
    async def get_document_by_id(doc_id: uuid.UUID) -> dict | None:
        """Get document by ID."""
        async with get_session() as session:
            result = await session.get(Document, doc_id)
            if result:
                return {
                    "id": str(result.id),
                    "user_id": str(result.user_id),
                    "name": result.name,
                    "doc_type": result.doc_type,
                    "source": result.source,
                    "uri": result.uri,
                    "size": result.size,
                    "size_bytes": result.size_bytes,
                    "tags": result.tags or [],
                    "status": result.status,
                    "metadata": result.document_metadata or {},
                    "created_at": result.created_at.isoformat() if result.created_at else None,
                    "updated_at": result.updated_at.isoformat() if result.updated_at else None,
                }
            return None

    @staticmethod
    async def get_documents_by_user(user_id: uuid.UUID) -> list[dict]:
        """Get all documents for a user."""
        async with get_session() as session:
            from sqlalchemy import select

            stmt = select(Document).where(Document.user_id == user_id).order_by(Document.created_at.desc())
            result = await session.execute(stmt)
            docs = result.scalars().all()

            return [
                {
                    "id": str(doc.id),
                    "user_id": str(doc.user_id),
                    "name": doc.name,
                    "doc_type": doc.doc_type,
                    "source": doc.source,
                    "uri": doc.uri,
                    "size": doc.size,
                    "size_bytes": doc.size_bytes,
                    "tags": doc.tags or [],
                    "status": doc.status,
                    "metadata": doc.document_metadata or {},
                    "created_at": doc.created_at.isoformat() if doc.created_at else None,
                    "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
                }
                for doc in docs
            ]

    @staticmethod
    async def update_document_status(doc_id: uuid.UUID, status: str) -> bool:
        """Update document status."""
        async with get_session() as session:
            from sqlalchemy import update

            stmt = update(Document).where(Document.id == doc_id).values(status=status)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    @staticmethod
    async def delete_document(doc_id: uuid.UUID) -> bool:
        """Delete a document."""
        async with get_session() as session:
            from sqlalchemy import delete

            stmt = delete(Document).where(Document.id == doc_id)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0


class JobManager:
    """Manages job operations in PostgreSQL."""

    @staticmethod
    async def create_job(job_data: dict) -> uuid.UUID:
        """Create a new job."""
        async with get_session() as session:
            job = Job(**job_data)
            session.add(job)
            await session.commit()
            await session.refresh(job)
            return job.id

    @staticmethod
    async def get_job_by_id(job_id: uuid.UUID) -> dict | None:
        """Get job by ID."""
        async with get_session() as session:
            result = await session.get(Job, job_id)
            if result:
                return {
                    "id": str(result.id),
                    "source_id": str(result.source_id),
                    "user_id": str(result.user_id),
                    "status": result.status,
                    "source_type": result.source_type,
                    "job_type": result.job_type,
                    "progress": result.progress,
                    "error_message": result.error_message,
                    "started_at": result.started_at.isoformat() if result.started_at else None,
                    "completed_at": result.completed_at.isoformat()
                    if result.completed_at
                    else None,
                    "created_at": result.created_at.isoformat(),
                }
            return None

    @staticmethod
    async def update_job_status(
        job_id: uuid.UUID, status: str, progress: int = None, error_message: str = None
    ) -> bool:
        """Update job status."""
        async with get_session() as session:
            from sqlalchemy import update

            update_values = {"status": status}
            if progress is not None:
                update_values["progress"] = progress
            if error_message is not None:
                update_values["error_message"] = error_message
            if status == "running":
                update_values["started_at"] = func.now()
            elif status in ["completed", "failed"]:
                update_values["completed_at"] = func.now()

            stmt = update(Job).where(Job.id == job_id).values(**update_values)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    @staticmethod
    async def get_jobs_by_user(user_id: uuid.UUID, status: str = None) -> list[dict]:
        """Get jobs for a user, optionally filtered by status."""
        async with get_session() as session:
            from sqlalchemy import select

            stmt = select(Job).where(Job.user_id == user_id)
            if status:
                stmt = stmt.where(Job.status == status)
            stmt = stmt.order_by(Job.created_at.desc())

            result = await session.execute(stmt)
            jobs = result.scalars().all()

            return [
                {
                    "id": str(job.id),
                    "source_id": str(job.source_id),
                    "user_id": str(job.user_id),
                    "status": job.status,
                    "source_type": job.source_type,
                    "job_type": job.job_type,
                    "progress": job.progress,
                    "error_message": job.error_message,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "created_at": job.created_at.isoformat(),
                }
                for job in jobs
            ]
