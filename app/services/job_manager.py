"""
Data Connector Service - Job Manager
Manages processing jobs and tracks their progress using in-memory storage.
"""

import structlog
from datetime import datetime, timezone
from typing import Optional
import uuid

from app.models import (
    ProcessingJob,
    JobStatus,
    SourceType,
)

logger = structlog.get_logger()


class JobManager:
    """Manages processing jobs in memory."""

    def __init__(self):
        self._jobs: dict[str, ProcessingJob] = {}

    async def create_job(
        self,
        source_id: str,
        source_type: SourceType,
        metadata: dict | None = None,
    ) -> ProcessingJob:
        """
        Create a new processing job.

        Args:
            source_id: ID of the source to process
            source_type: Type of the source
            metadata: Optional job metadata

        Returns:
            The created job
        """
        now = datetime.now(timezone.utc)
        job_id = str(uuid.uuid4())

        job = ProcessingJob(
            id=job_id,
            source_id=source_id,
            source_type=source_type.value,
            status=JobStatus.PENDING.value,
            total_files=0,
            processed_files=0,
            code_files=0,
            document_files=0,
            errors=[],
            created_at=now,
            updated_at=now,
            completed_at=None,
            metadata=metadata or {},
        )

        self._jobs[job_id] = job

        logger.info(
            "Created processing job",
            job_id=job_id,
            source_id=source_id,
            source_type=source_type.value,
        )

        return job

    async def get_job(self, job_id: str) -> Optional[ProcessingJob]:
        """
        Get a job by ID.

        Args:
            job_id: The job ID

        Returns:
            The job if found, None otherwise
        """
        return self._jobs.get(job_id)

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        processed_files: int | None = None,
        code_files: int | None = None,
        document_files: int | None = None,
    ) -> bool:
        """
        Update job status and progress.

        Args:
            job_id: The job ID
            status: New status
            processed_files: Updated processed file count
            code_files: Updated code file count
            document_files: Updated document file count

        Returns:
            True if updated, False otherwise
        """
        job = self._jobs.get(job_id)
        if not job:
            return False

        job.status = status.value
        job.updated_at = datetime.now(timezone.utc)

        if processed_files is not None:
            job.processed_files = processed_files
        if code_files is not None:
            job.code_files = code_files
        if document_files is not None:
            job.document_files = document_files

        if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            job.completed_at = datetime.now(timezone.utc)

        logger.info(
            "Updated job status",
            job_id=job_id,
            status=status.value,
        )

        return True

    async def list_jobs(
        self,
        source_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ProcessingJob]:
        """
        List jobs with optional filtering.

        Args:
            source_id: Filter by source ID
            status: Filter by status
            limit: Maximum jobs to return
            offset: Number of jobs to skip

        Returns:
            List of jobs
        """
        jobs = list(self._jobs.values())

        # Filter by source_id
        if source_id:
            jobs = [j for j in jobs if j.source_id == source_id]

        # Filter by status
        if status:
            jobs = [j for j in jobs if j.status == status.value]

        # Sort by created_at descending
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        # Apply pagination
        return jobs[offset : offset + limit]


# Global job manager instance
_job_manager: JobManager | None = None


def get_job_manager() -> JobManager:
    """Get the global job manager instance."""
    global _job_manager
    if _job_manager is None:
        _job_manager = JobManager()
    return _job_manager
