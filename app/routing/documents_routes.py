"""
Document Management Routes for Data Connector Service
Handles document CRUD operations, file uploads, and analytics
"""

import uuid
from datetime import datetime, timezone
from typing import List, Optional
import structlog
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    UploadFile,
    File,
    Form,
    BackgroundTasks,
    Request,
)
from pydantic import BaseModel
from pathlib import Path
from app.utils.user import parse_user_id

from app.infra.db.postgres import get_session, Source
from app.config import get_settings
from app.services.documents.ingester import trigger_initial_sync

logger = structlog.get_logger()
router = APIRouter(prefix="/api/documents", tags=["documents"])

# In-memory store for demo data
_documents = []

# --------------- Models ---------------


class CreateDocumentRequest(BaseModel):
    name: str
    doc_type: str
    source: str
    size: Optional[str] = None
    tags: Optional[list[str]] = None


# =============================================================================
# Document CRUD Endpoints
# =============================================================================


@router.get("")
async def list_documents(
    http_request: Request,
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """List all documents with optional search filtering and pagination."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required.")

    async with get_session() as session:
        from sqlalchemy import select

        # Consider these types as "documents"
        doc_types = [
            "upload",
            "google_drive",
            "gdrive",
            "dropbox",
            "onedrive",
            "notion",
            "confluence",
        ]

        # Query for document-related sources
        try:
            # Handle potential UUID formatting issues
            query = select(Source).where(
                Source.type.in_(doc_types), Source.user_id == parse_user_id(user_id)
            )
        except ValueError:
            query = select(Source).where(Source.type.in_(doc_types), Source.user_id == user_id)

        result = await session.execute(query)
        sources = result.scalars().all()

        # Convert sources to document-like dictionaries
        db_docs = []
        for s in sources:
            meta = s.source_metadata or {}
            files = meta.get("files", [])
            if files:
                for f in files:
                    db_docs.append(
                        {
                            "id": str(s.id) + "-" + f.get("name", "Unknown"),
                            "user_id": str(s.user_id),
                            "name": f.get("name", "Unknown"),
                            "doc_type": f.get(
                                "content_type",
                                f.get("name", "").split(".")[-1]
                                if "." in f.get("name", "")
                                else "unknown",
                            ),
                            "source": s.type,
                            "size": f"{f.get('size_bytes', 0) / 1024:.1f} KB"
                            if f.get("size_bytes")
                            else "Unknown",
                            "tags": meta.get("tags", []),
                            "status": s.status,
                            "created_at": s.created_at.isoformat() if s.created_at else None,
                            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
                            "source_id": str(s.id),
                        }
                    )
            else:
                metadata_dict = meta.get("metadata", {})
                is_cloud_file = metadata_dict.get("isCloudFile", False)
                has_item_ids = bool(metadata_dict.get("item_ids"))
                # Only show dummy documents for single uploads or explicit cloud files.
                # Do not show dummy documents for entire cloud connections (like Dropbox) that are pending.
                if is_cloud_file or has_item_ids or (s.type == "upload" and s.status == "pending"):
                    db_docs.append(
                        {
                            "id": str(s.id) + "-dummy",
                            "user_id": str(s.user_id),
                            "name": s.name,
                            "doc_type": s.name.split(".")[-1] if "." in s.name else "unknown",
                            "source": s.type,
                            "size": "Unknown",
                            "tags": meta.get("tags", []),
                            "status": s.status,
                            "created_at": s.created_at.isoformat() if s.created_at else None,
                            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
                            "source_id": str(s.id),
                        }
                    )


        # Combine with in-memory documents
        all_docs = db_docs + [d for d in _documents if d.get("user_id") == user_id]

        if search:
            all_docs = [d for d in all_docs if search.lower() in d["name"].lower()]

        # Pagination
        total = len(all_docs)
        start_idx = (page - 1) * limit
        paginated_docs = all_docs[start_idx : start_idx + limit]

        return {
            "success": True,
            "message": "Documents retrieved successfully",
            "data": {
                "data": paginated_docs,
                "total": total,
                "page": page,
                "limit": limit,
                "total_pages": (total + limit - 1) // limit,
            },
        }


@router.post("", status_code=201)
async def create_document(payload: CreateDocumentRequest, http_request: Request):
    """Create a new document entry."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required.")

    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "name": payload.name,
        "doc_type": payload.doc_type,
        "source": payload.source,
        "size": payload.size or "0 KB",
        "tags": payload.tags or [],
        "status": "active",
        "created_at": now,
        "updated_at": now,
    }
    _documents.append(doc)
    return {"success": True, "message": "Document created successfully", "data": doc}


@router.delete("/{doc_id:path}")
async def delete_document(doc_id: str):
    """Delete a document by ID (handles both demo and database sources)."""
    global _documents

    # 1. Try deleting from in-memory store (demo data)
    before = len(_documents)
    _documents = [d for d in _documents if d["id"] != doc_id]
    if len(_documents) < before:
        return {"success": True, "message": "Demo document deleted successfully"}

    # 2. Try deleting from database (real data)
    try:
        # Check if it's a valid UUID
        # Sometimes doc_id is composite: {uuid}-{filename}
        source_id_str = doc_id[:36] if len(doc_id) >= 36 else doc_id
        val_uuid = uuid.UUID(source_id_str)
        async with get_session() as session:
            from sqlalchemy import delete
            from app.infra.db.postgres import Source

            # Use delete statement for efficiency and get the deleted user_id
            stmt = delete(Source).where(Source.id == val_uuid).returning(Source.user_id)
            result = await session.execute(stmt)
            
            row = result.fetchone()
            await session.commit()

            if row is not None:
                # Trigger downstream graph cleanup
                from app.services.client import get_service_client
                client = get_service_client()
                import asyncio
                
                user_id_str = str(row[0]) if row[0] else "system"
                asyncio.create_task(client.delete_graph_group(source_id_str, user_id_str))
                return {"success": True, "message": "Database document deleted successfully"}

    except ValueError:
        # Not a UUID, skip DB check
        pass

    # 3. Not found in either
    raise HTTPException(status_code=404, detail="Document not found")


@router.get("/analytics")
async def get_analytics():
    """Get document analytics and statistics."""
    async with get_session() as session:
        from sqlalchemy import select

        # Consider these types as "documents"
        doc_types = [
            "upload",
            "google_drive",
            "gdrive",
            "dropbox",
            "onedrive",
            "notion",
            "confluence",
        ]

        # Count total documents and sum size
        query = select(Source).where(Source.type.in_(doc_types))
        result = await session.execute(query)
        sources = result.scalars().all()

        total_documents = 0
        total_size_bytes = 0
        by_source = {}
        by_type = {}

        for s in sources:
            meta = s.source_metadata or {}
            files = meta.get("files", [])
            file_count = len(files)

            metadata_dict = meta.get("metadata", {}) or {}
            item_ids = metadata_dict.get("item_ids") or meta.get("item_ids")
            is_cloud_file = metadata_dict.get("isCloudFile", False)
            pending_upload = s.type == "upload" and s.status == "pending"

            if file_count == 0 and (item_ids or is_cloud_file or pending_upload):
                if isinstance(item_ids, list) and item_ids:
                    file_count = len(item_ids)
                else:
                    file_count = 1

            total_documents += file_count
            size = meta.get("size_bytes", 0)
            total_size_bytes += size

            src_type = s.type
            if file_count > 0:
                by_source[src_type] = by_source.get(src_type, 0) + file_count

            if files:
                for f in files:
                    fname = f.get("name", "")
                    ftype = f.get(
                        "content_type", fname.split(".")[-1] if "." in fname else "unknown"
                    )
                    by_type[ftype] = by_type.get(ftype, 0) + 1
            elif file_count > 0:
                dtype = s.name.split(".")[-1] if s.name and "." in s.name else "unknown"
                by_type[dtype] = by_type.get(dtype, 0) + file_count

        # Add in-memory documents (demo data)
        total_documents += len(_documents)
        for doc in _documents:
            src = doc.get("source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1

            dtype = doc.get("doc_type", "unknown")
            by_type[dtype] = by_type.get(dtype, 0) + 1

            # Assume some size for demo docs or parse it
            # doc["size"] might be "45 KB"
            size_str = doc.get("size", "0 KB")
            try:
                if "KB" in size_str:
                    total_size_bytes += float(size_str.replace(" KB", "")) * 1024
                elif "MB" in size_str:
                    total_size_bytes += float(size_str.replace(" MB", "")) * 1024 * 1024
            except:
                pass

        return {
            "success": True,
            "message": "Analytics retrieved successfully",
            "data": {
                "total_documents": total_documents,
                "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "by_type": by_type,
                "by_source": by_source,
            },
        }


# =============================================================================
# File Upload Endpoints
# =============================================================================


@router.post("/upload")
async def upload_documents(
    http_request: Request,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    source_name: str = Form(default="local-upload"),
):
    """
    Upload documents directly to the data-connector service.

    This endpoint accepts file uploads, saves them to the shared downloads volume,
    creates a Source record in the database, and triggers the unified-processor
    for ingestion and semantic processing.
    """
    user_id_str = http_request.headers.get("x-user-id")
    if not user_id_str:
        raise HTTPException(status_code=401, detail="User authentication required.")

    if user_id_str:
        user_id = parse_user_id(user_id_str)
    else:
        user_id = parse_user_id(None)

    settings = get_settings()
    source_id = uuid.uuid4()

    # Path where files will be stored: {DOWNLOADS_BASE_PATH}/docs/{source_id}/
    downloads_base = Path(settings.downloads_folder)
    target_dir = downloads_base / "docs" / str(source_id)

    logger.info(
        "Processing document upload",
        file_count=len(files),
        source_name=source_name,
        source_id=str(source_id),
        target_dir=str(target_dir),
    )

    try:
        # 1. Create target directory
        target_dir.mkdir(parents=True, exist_ok=True)

        total_size = 0
        uploaded_files_info = []

        # 2. Save each file to the shared volume
        for file in files:
            file_content = await file.read()
            file_size = len(file_content)
            total_size += file_size

            # Use original filename
            file_path = target_dir / file.filename

            # Basic path traversal protection
            if not str(file_path.resolve()).startswith(str(target_dir.resolve())):
                logger.warning("Potential path traversal attempt blocked", filename=file.filename)
                continue

            with open(file_path, "wb") as f:
                f.write(file_content)

            uploaded_files_info.append(
                {"name": file.filename, "size_bytes": file_size, "content_type": file.content_type}
            )

            logger.debug("Saved file locally", filename=file.filename, size=file_size)

        # 3. Create Source record in PostgreSQL
        async with get_session() as session:
            new_source = Source(
                id=source_id,
                user_id=user_id,
                name=source_name
                if source_name != "local-upload"
                else files[0].filename
                if files
                else "uploaded-files",
                type="upload",
                uri=f"local://docs/{source_id}",
                source_metadata={
                    "files": uploaded_files_info,
                    "total_size_bytes": total_size,
                    "file_count": len(uploaded_files_info),
                    "upload_time": datetime.now(timezone.utc).isoformat(),
                },
                status="active",
            )

            session.add(new_source)
            await session.commit()

            logger.info("Source record created in database", source_id=str(source_id))

            # Update billing count
            try:
                import httpx
                settings = get_settings()
                async with httpx.AsyncClient() as http_client:
                    await http_client.post(
                        f"{settings.auth_url}/billing/internal/update-doc-count",
                        json={"userId": user_id, "delta": 1},
                        headers={"X-API-Key": settings.internal_api_key},
                        timeout=10.0
                    )
                logger.info("[DOC-CREATE] Updated billing doc count", user_id=user_id)
            except Exception as e:
                logger.warning("[DOC-CREATE] Failed to update billing count", error=str(e))

        # 4. Trigger the initial sync background task
        # This will verify the directory and then hit unified-processor's /api/v1/process/local
        background_tasks.add_task(
            trigger_initial_sync, str(source_id), "upload", new_source.source_metadata
        )

        return {
            "success": True,
            "message": f"Successfully uploaded {len(uploaded_files_info)} file(s) and triggered processing",
            "data": {
                "source_id": str(source_id),
                "files_processed": len(uploaded_files_info),
                "files_received": len(files),
                "total_size_bytes": total_size,
            },
        }

    except Exception as e:
        logger.error("Failed to process document upload", error=str(e), exc_info=True)
        # Cleanup directory if it was created but failed mid-way
        if target_dir.exists():
            import shutil

            try:
                shutil.rmtree(target_dir)
            except:
                pass

        raise HTTPException(status_code=500, detail=f"Upload processing failed: {str(e)}")
