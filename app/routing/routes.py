"""
Data Connector Service - API Routes
Merged from external_routes.py, internal_routes.py, and routes.py
"""

import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional
import structlog
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Header, Query
from app.utils.user import parse_user_id
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from app.config import get_settings

logger = structlog.get_logger()

from app.models import (
    SourceResponse,
    SourceType,
    JobStatus,
    IngestRequest,
)
from app.infra.db.postgres import get_session, Document
from app.router import get_router
from app.services import get_job_manager, get_service_client
from app.config import get_settings
from app.utils.validation import InputValidator, ValidationError

from app.services.client import ServiceClient

logger = structlog.get_logger()
router = APIRouter()
settings = get_settings()



# =============================================================================
# Request/Response Models
# =============================================================================


class HealthResponse(BaseModel):
    status: str = "healthy"
    service: str = "data-connector"
    version: str = "2.0.0"
    timestamp: str


class SourceCreateRequest(BaseModel):
    """Request to create a new source."""

    type: SourceType
    name: str
    uri: str
    credentials: dict[str, Any] | None = None
    branch: str | None = None
    include_patterns: list[str] = ["**/*"]
    exclude_patterns: list[str] = []
    metadata: dict[str, Any] = {}

    def validate(self) -> None:
        """Validate the request data."""
        try:
            if not self.name or len(self.name.strip()) == 0:
                raise ValidationError("Source name is required")
            if len(self.name) > 255:
                raise ValidationError("Source name must be less than 255 characters")
            if self.type in ["github", "gitlab", "bitbucket"]:
                self.uri = InputValidator.validate_repository_url(self.uri)
            else:
                if not self.uri or len(self.uri.strip()) == 0:
                    raise ValidationError("URI is required")
            if self.type in ["github", "gitlab", "bitbucket"]:
                self.branch = InputValidator.validate_branch_name(self.branch)
            self.include_patterns = InputValidator.validate_include_patterns(self.include_patterns)
            self.exclude_patterns = InputValidator.validate_exclude_patterns(self.exclude_patterns)
            if self.credentials:
                self.credentials = InputValidator.validate_oauth_credentials(self.credentials)
        except ValidationError as e:
            raise ValidationError(f"Validation failed: {str(e)}")
        self.metadata = InputValidator.sanitize_metadata(self.metadata)


class RouteFileRequest(BaseModel):
    file_paths: list[str]


class RouteFileResponse(BaseModel):
    code_files: list[str]
    document_files: list[str]
    unknown_files: list[str]
    total: int


# External Routes Models
class UrlCreateRequest(BaseModel):
    url: str
    title: str = ""
    description: str = ""
    tags: List[str] = []

class UrlUpdateRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    tags: List[str] | None = None

class GoogleDriveCallbackRequest(BaseModel):
    code: str


class GoogleDriveSyncRequest(BaseModel):
    folder_id: str | None = None
    include_patterns: List[str] = ["**/*"]
    exclude_patterns: List[str] = []


class DropboxCallbackRequest(BaseModel):
    code: str


class DropboxSyncRequest(BaseModel):
    folder_path: str | None = None
    include_patterns: List[str] = ["**/*"]
    exclude_patterns: List[str] = []


# Internal Routes Models
class CredentialExchangeRequest(BaseModel):
    credential_ref: str


class CredentialExchangeResponse(BaseModel):
    provider: str
    access_token: str
    refresh_token: str | None = None
    expires_at: str | None = None


# =============================================================================
# Health & Status Endpoints
# =============================================================================


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        service="data-connector",
        version="2.0.0",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/api/v1/status")
async def get_status():
    """Get service status and downstream service health."""
    client = get_service_client()
    downstream_status = {}
    for service in ["unified-processor", "embeddings-service"]:
        downstream_status[service] = await client.check_service_health(service)

    return {
        "service": "data-connector",
        "version": "2.0.0",
        "status": "running",
        "downstream_services": downstream_status,
    }


# =============================================================================
# Source Management Endpoints
# =============================================================================


@router.post("/api/sources", response_model=SourceResponse)
async def create_source(
    request: SourceCreateRequest, http_request: Request, background_tasks: BackgroundTasks
):
    """Create a new document source (stores in documents table)."""
    logger.info(
        "[SOURCE-CREATE] Starting document source creation",
        name=request.name,
        type=request.type.value,
        uri=request.uri,
    )
    try:
        request.validate()
    except ValidationError as e:
        logger.warning("Request validation failed", error=str(e))
        raise
    except Exception as e:
        logger.error("Unexpected validation error", error=str(e))
        raise HTTPException(status_code=500, detail="Validation error")

    # Reject repository types — those must go through repo-data-con
    if request.type.value in ["github", "gitlab", "bitbucket"]:
        raise HTTPException(
            status_code=400,
            detail="Repository sources must be created via repo-data-con /api/repositories",
        )

    async with get_session() as session:
        # Auto-fetch OAuth tokens from auth-middleware for providers that need them
        providers_needing_tokens = [
            "google-drive",
            "onedrive",
            "gdrive",
            "dropbox",
            "slack",
            "notion",
            "custom",
        ]
        if not request.credentials and request.type.value in providers_needing_tokens:
            user_id = http_request.headers.get("x-user-id")
            if not user_id:
                raise HTTPException(status_code=401, detail="User authentication required")
            client = get_service_client()
            try:
                provider_name = request.type.value
                if provider_name in ("gdrive", "google-drive"):
                    provider_name = "google"
                elif provider_name == "custom":
                    provider_name = "custom_apps"

                tokens = await client.get_auth_token(user_id, provider_name)
                if not tokens or not tokens.get("access_token"):
                    raise HTTPException(
                        status_code=401, detail=f"No OAuth tokens found for {request.type.value}"
                    )
                request.credentials = tokens
            except HTTPException:
                raise
            except Exception as e:
                logger.error("Failed to retrieve OAuth tokens", error=str(e))
                raise HTTPException(status_code=503, detail="Authentication service unavailable")

        from sqlalchemy import select as sa_select

        request_user_id = parse_user_id(
            http_request.headers.get("x-user-id")
        )

        existing = await session.execute(sa_select(Document).where(Document.uri == request.uri, Document.user_id == request_user_id))
        existing_doc = existing.scalar_one_or_none()

        if existing_doc:
            logger.info(
                "[SOURCE-CREATE] Document already exists for same user, updating instead",
                doc_id=str(existing_doc.id),
            )
            existing_doc.name = request.name
            existing_doc.document_metadata = {
                "credentials": request.credentials,
                "branch": request.branch,
                "include_patterns": request.include_patterns,
                "exclude_patterns": request.exclude_patterns,
                "metadata": request.metadata,
            }
            existing_doc.status = "pending"
            existing_doc.updated_at = datetime.now(timezone.utc)

            try:
                await session.commit()
            except IntegrityError as e:
                await session.rollback()
                raise HTTPException(status_code=409, detail=f"Document already exists: {str(e)}")

            await session.refresh(existing_doc)

            from app.services.documents.ingester import trigger_initial_sync

            background_tasks.add_task(
                trigger_initial_sync,
                str(existing_doc.id),
                existing_doc.source,
                existing_doc.document_metadata,
            )

            return SourceResponse(
                id=str(existing_doc.id),
                type=SourceType(existing_doc.source),
                name=existing_doc.name,
                uri=existing_doc.uri,
                status=existing_doc.status,
                created_at=existing_doc.created_at,
                updated_at=existing_doc.updated_at,
                metadata=existing_doc.document_metadata or {},
                syncStarted=True,
            )

        doc = Document(
            user_id=request_user_id,
            doc_type=request.name.split(".")[-1] if "." in request.name else "unknown",
            source=request.type.value,
            uri=request.uri,
            name=request.name,
            document_metadata={
                "credentials": request.credentials,
                "branch": request.branch,
                "include_patterns": request.include_patterns,
                "exclude_patterns": request.exclude_patterns,
                "metadata": request.metadata,
            },
        )

        session.add(doc)

        try:
            await session.commit()
        except IntegrityError as e:
            await session.rollback()
            raise HTTPException(status_code=409, detail=f"Document already exists: {str(e)}")
        await session.refresh(doc)

        from app.services.documents.ingester import trigger_initial_sync

        background_tasks.add_task(
            trigger_initial_sync, str(doc.id), doc.source, doc.document_metadata
        )

        return SourceResponse(
            id=str(doc.id),
            type=SourceType(doc.source),
            name=doc.name,
            uri=doc.uri,
            status=doc.status,
            created_at=doc.created_at,
            updated_at=doc.updated_at,
            metadata=doc.document_metadata or {},
            syncStarted=True,
        )


@router.get("/api/sources")
async def list_sources(
    http_request: Request, type: SourceType | None = None, limit: int = 50, offset: int = 0
):
    """List all document sources."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    async with get_session() as session:
        from sqlalchemy import select

        query = select(Document).where(Document.user_id == parse_user_id(user_id))
        if type:
            query = query.where(Document.source == type.value)
        query = query.offset(offset).limit(limit)
        result = await session.execute(query)
        docs = result.scalars().all()

        return {
            "sources": [
                {
                    "id": str(doc.id),
                    "type": doc.source,
                    "name": doc.name,
                    "uri": doc.uri,
                    "status": doc.status,
                    "created_at": doc.created_at.isoformat() if doc.created_at else None,
                    "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
                    "metadata": doc.document_metadata or {},
                }
                for doc in docs
            ],
            "total": len(docs),
        }


@router.get("/api/sources/{source_id}")
async def get_source(source_id: str, http_request: Request):
    """Get a specific document source by ID."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    try:
        rid = uuid.UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source ID format")

    async with get_session() as session:
        doc = await session.get(Document, rid)
        if not doc or doc.user_id != parse_user_id(user_id):
            raise HTTPException(status_code=404, detail="Source not found")

        return {
            "id": str(doc.id),
            "type": doc.source,
            "name": doc.name,
            "uri": doc.uri,
            "status": doc.status,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
            "metadata": doc.document_metadata or {},
        }


@router.delete("/api/sources/{source_id}")
async def delete_source(source_id: str, http_request: Request):
    """Delete a document source."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    try:
        source_uuid = uuid.UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source ID format")

    async with get_session() as session:
        doc = await session.get(Document, source_uuid)
        if not doc:
            raise HTTPException(status_code=404, detail="Source not found")

        if doc.user_id != parse_user_id(user_id):
            raise HTTPException(status_code=404, detail="Source not found")

        await session.delete(doc)
        await session.commit()

        # Update billing count
        try:
            import httpx
            settings = get_settings()
            async with httpx.AsyncClient() as http_client:
                await http_client.post(
                    f"{settings.auth_url}/billing/internal/update-doc-count",
                    json={"userId": user_id, "delta": -1},
                    headers={"X-API-Key": settings.internal_api_key},
                    timeout=10.0
                )
            logger.info("[DOC-DELETE] Updated billing doc count", user_id=user_id)
        except Exception as e:
            logger.warning("[DOC-DELETE] Failed to update billing count", error=str(e))

        from app.services.client import get_service_client
        client = get_service_client()
        import asyncio

        asyncio.create_task(client.delete_graph_group(source_id, user_id))

        return {"success": True, "message": "Document deleted successfully"}


@router.post("/api/sources/{source_id}/sync")
async def sync_source_endpoint(source_id: str, background_tasks: BackgroundTasks):
    """Trigger a manual sync for a source."""
    return await start_ingestion(IngestRequest(source_id=source_id), background_tasks)


# =============================================================================
# External API Routes (Cloud Storage & URLs)
# =============================================================================


@router.post("/api/v1/external/urls")
async def create_url(request: UrlCreateRequest, http_request: Request):
    user_id = parse_user_id(http_request.headers.get("x-user-id"))
    async with get_session() as session:
        new_doc = Document(
            user_id=user_id,
            doc_type="url",
            source="url",
            uri=request.url,
            name=request.title or request.url,
            document_metadata={"description": request.description, "tags": request.tags},
            status="active",
        )
        session.add(new_doc)
        await session.commit()
        await session.refresh(new_doc)
        return {"success": True, "id": str(new_doc.id)}

@router.get("/api/v1/external/urls")
async def get_urls(http_request: Request):
    user_id = parse_user_id(http_request.headers.get("x-user-id"))
    async with get_session() as session:
        from sqlalchemy import select as sa_select
        result = await session.execute(sa_select(Document).where(Document.user_id == user_id, Document.source == "url"))
        docs = result.scalars().all()
        return {"data": [{"id": str(d.id), "url": d.uri, "title": d.name, "description": (d.document_metadata or {}).get("description", ""), "tags": (d.document_metadata or {}).get("tags", []), "status": d.status, "created_at": d.created_at} for d in docs]}

@router.delete("/api/v1/external/urls/{url_id}")
async def delete_url(url_id: str, http_request: Request):
    user_id = parse_user_id(http_request.headers.get("x-user-id"))
    try:
        url_uuid = uuid.UUID(url_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid URL ID format")

    async with get_session() as session:
        from sqlalchemy import select as sa_select
        result = await session.execute(sa_select(Document).where(Document.id == url_uuid, Document.user_id == user_id))
        doc = result.scalar_one_or_none()
        if not doc:
            raise HTTPException(status_code=404, detail="URL not found")
        await session.delete(doc)
        await session.commit()
        return {"success": True}

@router.put("/api/v1/external/urls/{url_id}")
async def update_url(url_id: str, request: UrlUpdateRequest, http_request: Request):
    user_id = parse_user_id(http_request.headers.get("x-user-id"))
    try:
        url_uuid = uuid.UUID(url_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid URL ID format")

    async with get_session() as session:
        from sqlalchemy import select as sa_select
        result = await session.execute(sa_select(Document).where(Document.id == url_uuid, Document.user_id == user_id))
        doc = result.scalar_one_or_none()
        if not doc:
            raise HTTPException(status_code=404, detail="URL not found")
        if request.title is not None:
            doc.name = request.title
        metadata = doc.document_metadata or {}
        if request.description is not None:
            metadata["description"] = request.description
        if request.tags is not None:
            metadata["tags"] = request.tags
        doc.document_metadata = metadata
        await session.commit()
        return {"success": True}

@router.get("/api/v1/external/browse/{provider}")
async def browse_provider_files(provider: str, http_request: Request, path: Optional[str] = ""):
    """Browse files for a specific cloud provider to allow selective sync."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    client = get_service_client()
    try:
        provider_name = provider
        if provider == "gdrive":
            provider_name = "google"

        tokens = None
        try:
            tokens = await client.get_auth_token(user_id, provider_name)
        except HTTPException as e:
            # Safety-net fallback: auth-middleware now resolves provider aliases internally,
            # but keep this in case of older auth-middleware deployments.
            if provider == "onedrive" and e.status_code in (401, 404):
                try:
                    tokens = await client.get_auth_token(user_id, "windowslive")
                except HTTPException:
                    raise e  # raise the original exception
            else:
                raise

        if not tokens or not tokens.get("access_token"):
            raise HTTPException(
                status_code=401, detail=f"No active connection found for {provider}. Please connect your account."
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to retrieve OAuth tokens for browse", error=str(e))
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    try:
        if provider == "onedrive":
            from app.connectors.onedrive_client import OneDriveConnector

            connector = OneDriveConnector(settings)
            connector.set_credentials(tokens.get("access_token"), tokens.get("refresh_token"))

            folder_id = None
            # Backward compatibility: if path looks like an ID, use it as folder_id
            if path and not folder_id and ("!" in path or len(path) > 20):
                folder_id = path
                path = ""

            items = await connector.list_drive_items(path=path or "", folder_id=folder_id or None)

            # Standardize output format for the UI
            files = []
            for item in items:
                files.append(
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "path": item.get("path", ""),
                        "type": item["type"],  # 'file' or 'folder'
                        "size": item.get("size"),
                        "mime_type": item.get("mimeType"),
                        "last_modified": item.get("lastModifiedDateTime"),
                    }
                )
            return {"success": True, "data": files, "path": path}

        elif provider == "gdrive":
            from app.connectors.gdrive_client import GoogleDriveConnector

            connector = GoogleDriveConnector(settings)
            connector.set_credentials(tokens["access_token"], tokens.get("refresh_token"))

            result = await connector.list_files(folder_id=path if path else "root")

            mapped = []
            for item in result.get("files", []):
                mapped.append(
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "path": "",
                        "type": "folder"
                        if item.get("mime_type") == "application/vnd.google-apps.folder"
                        else "file",
                        "size": item.get("size"),
                        "mime_type": item.get("mime_type"),
                        "last_modified": item.get("modified_at"),
                    }
                )
            return {"success": True, "data": mapped, "path": path}

        elif provider == "dropbox":
            from app.connectors.dropbox_client import DropboxConnector

            connector = DropboxConnector(settings)
            connector.set_credentials(tokens["access_token"], tokens.get("refresh_token"))

            items = await connector.list_folder(path=path)

            mapped = []
            for item in items:
                mapped.append(
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "path": item.get("path", ""),
                        "type": item["type"],
                        "size": item.get("size"),
                        "mime_type": None,
                        "last_modified": item.get("server_modified") or item.get("client_modified"),
                    }
                )
            return {"success": True, "data": mapped, "path": path}

        else:
            raise HTTPException(
                status_code=400, detail=f"Provider {provider} not supported for browsing yet"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error browsing files", provider=provider, path=path, error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch files from {provider}: {str(e)}"
        )


@router.post("/api/v1/external/gdrive/callback")
async def google_drive_callback(request: GoogleDriveCallbackRequest):
    """Handle OAuth callback from Google Drive."""
    try:
        from app.connectors.gdrive_client import GDriveConnector

        connector = GDriveConnector(settings)
        token_data = await connector.exchange_code_for_token(request.code)
        return {
            "success": True,
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token"),
            "expires_in": token_data.get("expires_in"),
            "message": "Google Drive connected successfully",
        }
    except Exception as e:
        logger.error("Google Drive OAuth callback error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/external/gdrive/sync")
async def sync_google_drive(request: GoogleDriveSyncRequest, background_tasks: BackgroundTasks):
    """Sync files from Google Drive."""
    try:
        async with get_session() as session:
            doc = Document(
                user_id=parse_user_id(None),
                doc_type="gdrive",
                source="gdrive",
                name="Google Drive Sync",
                uri="gdrive://sync",
                document_metadata={
                    "folder_id": request.folder_id,
                    "include_patterns": request.include_patterns,
                    "exclude_patterns": request.exclude_patterns,
                },
            )
            session.add(doc)
            await session.commit()
            await session.refresh(doc)

            background_tasks.add_task(
                sync_gdrive_background,
                str(doc.id),
                request.folder_id,
                request.include_patterns,
                request.exclude_patterns,
            )

            return {"source_id": str(doc.id), "message": "Google Drive sync started"}
    except Exception as e:
        logger.error("Google Drive sync error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/external/dropbox/callback")
async def dropbox_callback(request: DropboxCallbackRequest):
    """Handle OAuth callback from Dropbox."""
    try:
        from app.connectors.dropbox_client import DropboxConnector

        connector = DropboxConnector(settings)
        token_data = await connector.exchange_code_for_token(request.code)
        return {
            "success": True,
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token"),
            "expires_in": token_data.get("expires_in"),
            "message": "Dropbox connected successfully",
        }
    except Exception as e:
        logger.error("Dropbox OAuth callback error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/external/dropbox/sync")
async def sync_dropbox(request: DropboxSyncRequest, background_tasks: BackgroundTasks):
    """Sync files from Dropbox."""
    try:
        async with get_session() as session:
            doc = Document(
                user_id=parse_user_id(None),
                doc_type="dropbox",
                source="dropbox",
                name="Dropbox Sync",
                uri="dropbox://sync",
                document_metadata={
                    "folder_path": request.folder_path,
                    "include_patterns": request.include_patterns,
                    "exclude_patterns": request.exclude_patterns,
                },
            )
            session.add(doc)
            await session.commit()
            await session.refresh(doc)

            background_tasks.add_task(
                sync_dropbox_background,
                str(doc.id),
                request.folder_path,
                request.include_patterns,
                request.exclude_patterns,
            )

            return {"source_id": str(doc.id), "message": "Dropbox sync started"}
    except Exception as e:
        logger.error("Dropbox sync error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Internal API Routes (Webhooks & Credentials)
# =============================================================================




@router.get("/api/v1/internal/oauth/token")
async def get_oauth_token(
    request: Request,
    source_id: str = Query(...),
    x_internal_api_key: str = Header(..., alias="X-Internal-Api-Key"),
):
    """Retrieve a decrypted access token for a source."""
    if x_internal_api_key != settings.internal_api_key:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    try:
        from sqlalchemy import select

        async with get_session() as session:
            query = select(Document).where(Document.id == uuid.UUID(source_id))
            result = await session.execute(query)
            doc = result.scalar_one_or_none()

            if not doc:
                raise HTTPException(status_code=404, detail="Document not found")

            metadata = doc.document_metadata or {}
            credentials = metadata.get("credentials", {})

            if not credentials or not credentials.get("access_token"):
                raise HTTPException(status_code=404, detail="No access token found")

            return {
                "source_id": source_id,
                "access_token": credentials["access_token"],
                "token_type": credentials.get("token_type", "Bearer"),
                "expires_at": credentials.get("expires_at"),
                "scope": credentials.get("scope"),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to retrieve OAuth token", source_id=source_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/v1/internal/credentials/exchange", response_model=CredentialExchangeResponse)
async def exchange_credential_ref(
    request: CredentialExchangeRequest,
    x_internal_api_key: str = Header(..., alias="X-Internal-Api-Key"),
):
    """Exchange credential_ref JWT for actual credentials."""
    if x_internal_api_key != settings.internal_api_key:
        logger.warning("Invalid internal API key for credential exchange")
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    try:
        from app.security.credentials import get_jwt_generator
        from app.security.credentials import get_credential_storage
        import jwt

        jwt_generator = get_jwt_generator()

        try:
            claims = jwt_generator.verify_credential_ref(request.credential_ref)
        except jwt.ExpiredSignatureError:
            logger.warning("Expired credential_ref JWT")
            raise HTTPException(status_code=400, detail="Credential reference expired")
        except jwt.InvalidTokenError as e:
            logger.warning("Invalid credential_ref JWT", error=str(e))
            raise HTTPException(status_code=400, detail="Invalid credential reference")

        storage = get_credential_storage()
        credential = await storage.get_credential(claims.repo_id)

        if not credential:
            logger.warning(
                "Credential not found for exchange",
                repo_id=claims.repo_id,
                provider=claims.provider,
            )
            raise HTTPException(status_code=404, detail="Credential not found or expired")

        if credential.provider != claims.provider:
            logger.error(
                "Provider mismatch in credential exchange",
                jwt_provider=claims.provider,
                stored_provider=credential.provider,
            )
            raise HTTPException(status_code=400, detail="Provider mismatch")

        return CredentialExchangeResponse(
            provider=credential.provider,
            access_token=credential.access_token,
            refresh_token=credential.refresh_token,
            expires_at=credential.expires_at.isoformat() if credential.expires_at else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to exchange credential_ref", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/v1/internal/health")
async def internal_health():
    """Internal health check for service-to-service communication."""
    return {
        "status": "healthy",
        "service": "data-connector-internal",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Routing Endpoints
# =============================================================================


@router.post("/api/v1/route", response_model=RouteFileResponse)
async def route_files(request: RouteFileRequest):
    """Categorize files and determine routing."""
    file_router = get_router()
    code_files, document_files, unknown_files = file_router.categorize_files(request.file_paths)
    return RouteFileResponse(
        code_files=code_files,
        document_files=document_files,
        unknown_files=unknown_files,
        total=len(request.file_paths),
    )


@router.get("/api/v1/route/{file_path:path}")
async def route_single_file(file_path: str):
    """Get routing decision for a single file."""
    file_router = get_router()
    decision = file_router.route_file(file_path)
    return {
        "file_path": decision.file_path,
        "file_type": decision.file_type.value,
        "target_service": decision.target_service,
        "target_url": decision.target_url,
    }


# =============================================================================
# Job Management Endpoints
# =============================================================================


@router.post("/api/v1/ingest")
async def start_ingestion(request: IngestRequest, background_tasks: BackgroundTasks):
    """Start ingesting a document source."""
    async with get_session() as session:
        from sqlalchemy import select

        job_manager = get_job_manager()

        try:
            rid = uuid.UUID(request.source_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid source ID")

        query = select(Document).where(Document.id == rid)
        result = await session.execute(query)
        doc = result.scalar_one_or_none()

        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        job = await job_manager.create_job(
            source_id=request.source_id,
            source_type=SourceType(doc.source) if doc.source in [t.value for t in SourceType] else SourceType.UPLOAD,
            metadata={"force_reprocess": request.force_reprocess},
        )

        background_tasks.add_task(process_source_background, job.id, request.source_id, doc)

        return {"job_id": job.id, "status": job.status.value, "message": "Ingestion started"}


async def process_source_background(job_id: str, source_id: str, doc_obj):
    """Background task to process a document source."""
    job_manager = get_job_manager()
    try:
        await job_manager.update_job_status(job_id, JobStatus.PROCESSING)

        async with get_session() as session:
            from sqlalchemy import select
            import uuid

            query = select(Document).where(Document.id == uuid.UUID(source_id))
            result = await session.execute(query)
            doc = result.scalar_one_or_none()
            if not doc:
                logger.error(
                    "Document not found in background task", job_id=job_id, source_id=source_id
                )
                await job_manager.update_job_status(job_id, JobStatus.FAILED)
                return

            source_type = doc.source
            uri = doc.uri
            metadata = doc.document_metadata or {}
            credentials = metadata.get("credentials", {})
            user_id = str(doc.user_id)

        logger.info(
            "Processing document", job_id=job_id, source_id=source_id, source_type=source_type
        )

        from app.services.documents.ingester import trigger_initial_sync
        await trigger_initial_sync(source_id, source_type, metadata)
        await job_manager.update_job_status(job_id, JobStatus.COMPLETED)

    except Exception as e:
        logger.error("Failed to process document", job_id=job_id, error=str(e))
        await job_manager.update_job_status(job_id, JobStatus.FAILED)


@router.get("/api/v1/jobs")
async def list_jobs(
    source_id: str | None = None, status: JobStatus | None = None, limit: int = 50, offset: int = 0
):
    """List processing jobs."""
    job_manager = get_job_manager()
    jobs = await job_manager.list_jobs(
        source_id=source_id, status=status, limit=limit, offset=offset
    )

    return {
        "jobs": [
            {
                "id": job.id,
                "source_id": job.source_id,
                "source_type": job.source_type.value,
                "status": job.status.value,
                "total_files": job.total_files,
                "processed_files": job.processed_files,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
            }
            for job in jobs
        ],
        "total": len(jobs),
    }


@router.get("/api/v1/jobs/{job_id}")
async def get_job(job_id: str):
    """Get a specific job by ID."""
    job_manager = get_job_manager()
    job = await job_manager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "id": job.id,
        "source_id": job.source_id,
        "source_type": job.source_type.value,
        "status": job.status.value,
        "total_files": job.total_files,
        "processed_files": job.processed_files,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
    }


# =============================================================================
# Background Tasks
# =============================================================================


async def sync_gdrive_background(
    source_id: str, folder_id: str | None, include_patterns: List[str], exclude_patterns: List[str]
):
    """Background task to sync Google Drive files."""
    try:
        from app.connectors.gdrive_client import GDriveConnector

        connector = GDriveConnector(settings)

        async with get_session() as session:
            from sqlalchemy import select

            query = select(Document).where(Document.id == uuid.UUID(source_id))
            result = await session.execute(query)
            doc = result.scalar_one_or_none()

            if not doc:
                logger.error(f"Document not found: {source_id}")
                return

            credentials = (doc.document_metadata or {}).get("credentials", {})

        import typing

        files = typing.cast(
            list,
            await connector.fetch_files(
                credentials=credentials,
                folder_id=folder_id,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            ),
        )

        # Direct publishing of file content to unified-processor has been
        # removed. Use HTTP client for synchronous processing instead.
        logger.warning(
            "Direct file publishing removed; triggering source sync instead", source_id=source_id
        )
        service_client = ServiceClient()
        await service_client.trigger_source_sync(
            source_id=source_id, source_type="google_drive", source_url=source_id, metadata={}
        )
        logger.info(f"Google Drive sync requested via event-driven pipeline for source {source_id}")
    except Exception as e:
        logger.error(f"Google Drive sync failed: {str(e)}")


async def sync_dropbox_background(
    source_id: str,
    folder_path: str | None,
    include_patterns: List[str],
    exclude_patterns: List[str],
):
    """Background task to sync Dropbox files."""
    try:
        from app.connectors.dropbox_client import DropboxConnector

        connector = DropboxConnector(settings)

        async with get_session() as session:
            from sqlalchemy import select

            query = select(Document).where(Document.id == uuid.UUID(source_id))
            result = await session.execute(query)
            doc = result.scalar_one_or_none()

            if not doc:
                logger.error(f"Document not found: {source_id}")
                return

            credentials = (doc.document_metadata or {}).get("credentials", {})

        import typing

        files = typing.cast(
            list,
            await connector.fetch_files(
                credentials=credentials,
                folder_path=folder_path,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            ),
        )

        # Direct publishing of file content to unified-processor has been
        # removed. Use HTTP client for synchronous processing instead.
        logger.warning(
            "Direct file publishing removed; triggering source sync instead", source_id=source_id
        )
        service_client = ServiceClient()
        await service_client.trigger_source_sync(
            source_id=source_id, source_type="dropbox", source_url=source_id, metadata={}
        )
        logger.info(f"Dropbox sync requested via event-driven pipeline for source {source_id}")
    except Exception as e:
        logger.error(f"Dropbox sync failed: {str(e)}")


# =============================================================================
# Feature Toggles (Exposed for Frontend via direct DB query)
# =============================================================================


@router.get("/api/v1/toggles")
async def get_all_toggles():
    """Fetch all feature toggles from the shared database."""
    from sqlalchemy import text

    try:
        async with get_session() as session:
            result = await session.execute(
                text(
                    'SELECT name, enabled, description, category, category_type as "categoryType", metadata FROM feature_toggles.toggles'
                )
            )

            toggles = {}
            for row in result.fetchall():
                toggles[row[0]] = {
                    "enabled": bool(row[1]),
                    "description": row[2],
                    "category": row[3],
                    "categoryType": row[4],
                    "metadata": row[5] or {},
                }

            return {
                "success": True,
                "data": toggles,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.error("Failed to fetch toggles from DB", error=str(e))
        raise HTTPException(status_code=500, detail="Database connection error")


@router.get("/api/v1/toggles/{name}")
async def get_toggle(name: str):
    """Fetch a specific feature toggle."""
    from sqlalchemy import text

    try:
        async with get_session() as session:
            result = await session.execute(
                text(
                    'SELECT name, enabled, description, category, category_type as "categoryType", metadata FROM feature_toggles.toggles WHERE name = :name'
                ),
                {"name": name},
            )
            row = result.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"Toggle {name} not found")

            return {
                "success": True,
                "data": {
                    "name": row[0],
                    "enabled": bool(row[1]),
                    "description": row[2],
                    "category": row[3],
                    "categoryType": row[4],
                    "metadata": row[5] or {},
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to fetch toggle from DB", toggle_name=name, error=str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
