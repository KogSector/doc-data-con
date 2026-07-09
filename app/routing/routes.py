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

from app.models import (
    SourceResponse,
    SourceType,
    JobStatus,
    IngestRequest,
)
from app.infra.db.postgres import get_session, Source
from app.router import get_router
from app.services import get_job_manager, get_service_client
from app.config import get_settings
from app.utils.validation import InputValidator, ValidationError
from app.services.repositories.webhook_handler import UniversalWebhookHandler
from app.services.client import ServiceClient

logger = structlog.get_logger()
router = APIRouter()
settings = get_settings()
webhook_handler = UniversalWebhookHandler()


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


@router.post("/api/v1/sources", response_model=SourceResponse)
async def create_source(
    request: SourceCreateRequest, http_request: Request, background_tasks: BackgroundTasks
):
    """Create a new data source."""
    logger.info(
        "[SOURCE-CREATE] Starting source creation",
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

    async with get_session() as session:
        # Auto-fetch OAuth tokens from auth-middleware for providers that need them
        providers_needing_tokens = [
            "github",
            "gitlab",
            "bitbucket",
            "google-drive",
            "onedrive",
            "gdrive",
            "dropbox",
            "slack",
            "notion",
            "custom",
        ]
        if not request.credentials and request.type in providers_needing_tokens:
            user_id = http_request.headers.get("x-user-id")
            if not user_id:
                raise HTTPException(status_code=401, detail="User authentication required")
            client = get_service_client()
            try:
                # Map source type to the provider name stored in auth-middleware
                provider_name = request.type.value
                if provider_name == "gdrive":
                    provider_name = "google"
                elif provider_name == "google-drive":
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

        existing = await session.execute(sa_select(Source).where(Source.uri == request.uri))
        existing_source = existing.scalar_one_or_none()

        request_user_id = parse_user_id(
            http_request.headers.get("x-user-id")
        )

        if existing_source:
            if existing_source.user_id == request_user_id:
                logger.info(
                    "[SOURCE-CREATE] Source already exists for same user, updating instead",
                    source_id=str(existing_source.id),
                )
                existing_source.name = request.name
                existing_source.source_metadata = {
                    "credentials": request.credentials,
                    "branch": request.branch,
                    "include_patterns": request.include_patterns,
                    "exclude_patterns": request.exclude_patterns,
                    "metadata": request.metadata,
                }
                existing_source.status = "pending"
                existing_source.updated_at = datetime.now(timezone.utc)

                from app.infra.db.postgres import Repository

                if request.type.value in ["github", "gitlab", "bitbucket"]:
                    existing_repo = await session.execute(
                        sa_select(Repository).where(Repository.url == request.uri)
                    )
                    existing_repo_obj = existing_repo.scalars().first()
                    if not existing_repo_obj:
                        repo = Repository(
                            user_id=str(request_user_id),
                            name=request.name,
                            provider=request.type.value,
                            url=request.uri,
                            branch=request.branch or "main",
                            source_id=str(existing_source.id),
                            status="pending",
                        )
                        session.add(repo)
                    else:
                        existing_repo_obj.status = "pending"
                        existing_repo_obj.source_id = str(existing_source.id)
                        existing_repo_obj.branch = request.branch or "main"
                        existing_repo_obj.name = request.name
                        existing_repo_obj.updated_at = datetime.now(timezone.utc)

                try:
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback()
                    raise HTTPException(status_code=409, detail=f"Source already exists: {str(e)}")

                await session.refresh(existing_source)

                if existing_source.type in ["github", "gitlab", "bitbucket"]:
                    from app.services.repositories.ingester import trigger_repo_sync

                    background_tasks.add_task(
                        trigger_repo_sync,
                        str(existing_source.id),
                        existing_source.type,
                        existing_source.source_metadata,
                    )
                else:
                    from app.services.documents.ingester import trigger_initial_sync

                    background_tasks.add_task(
                        trigger_initial_sync,
                        str(existing_source.id),
                        existing_source.type,
                        existing_source.source_metadata,
                    )

                return SourceResponse(
                    id=str(existing_source.id),
                    type=SourceType(existing_source.type),
                    name=existing_source.name,
                    uri=existing_source.uri,
                    status=existing_source.status,
                    created_at=existing_source.created_at,
                    updated_at=existing_source.updated_at,
                    metadata=existing_source.source_metadata or {},
                    syncStarted=True,
                )
            else:
                raise HTTPException(
                    status_code=409,
                    detail="This repository has already been connected by another user.",
                )

        source = Source(
            user_id=request_user_id,
            type=request.type.value,
            uri=request.uri,
            name=request.name,
            source_metadata={
                "credentials": request.credentials,
                "branch": request.branch,
                "include_patterns": request.include_patterns,
                "exclude_patterns": request.exclude_patterns,
                "metadata": request.metadata,
            },
        )

        session.add(source)
        await session.flush()

        from app.infra.db.postgres import Repository

        if request.type.value in ["github", "gitlab", "bitbucket"]:
            existing_repo = await session.execute(
                sa_select(Repository).where(Repository.url == request.uri)
            )
            existing_repo_obj = existing_repo.scalars().first()
            if not existing_repo_obj:
                repo = Repository(
                    user_id=str(request_user_id),
                    name=request.name,
                    provider=request.type.value,
                    url=request.uri,
                    branch=request.branch or "main",
                    source_id=str(source.id),
                    status="pending",
                )
                session.add(repo)
            else:
                existing_repo_obj.status = "pending"
                existing_repo_obj.source_id = str(source.id)

        try:
            await session.commit()
        except IntegrityError as e:
            await session.rollback()
            raise HTTPException(status_code=409, detail=f"Source already exists: {str(e)}")
        await session.refresh(source)

        if source.type in ["github", "gitlab", "bitbucket"]:
            from app.services.repositories.ingester import trigger_repo_sync

            background_tasks.add_task(
                trigger_repo_sync, str(source.id), source.type, source.source_metadata
            )
        else:
            from app.services.documents.ingester import trigger_initial_sync

            background_tasks.add_task(
                trigger_initial_sync, str(source.id), source.type, source.source_metadata
            )

        return SourceResponse(
            id=str(source.id),
            type=SourceType(source.type),
            name=source.name,
            uri=source.uri,
            status=source.status,
            created_at=source.created_at,
            updated_at=source.updated_at,
            metadata=source.source_metadata or {},
            syncStarted=True,
        )


@router.get("/api/v1/sources")
async def list_sources(
    http_request: Request, type: SourceType | None = None, limit: int = 50, offset: int = 0
):
    """List all data sources."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    async with get_session() as session:
        from sqlalchemy import select

        query = select(Source).where(Source.user_id == parse_user_id(user_id))
        if type:
            query = query.where(Source.type == type.value)
        query = query.offset(offset).limit(limit)
        result = await session.execute(query)
        sources = result.scalars().all()

        return {
            "sources": [
                {
                    "id": str(source.id),
                    "type": source.type,
                    "name": source.name,
                    "uri": source.uri,
                    "status": source.status,
                    "created_at": source.created_at.isoformat(),
                    "updated_at": source.updated_at.isoformat(),
                    "metadata": source.source_metadata or {},
                }
                for source in sources
            ],
            "total": len(sources),
        }


@router.get("/api/v1/sources/{source_id}")
async def get_source(source_id: str, http_request: Request):
    """Get a specific source by ID."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    async with get_session() as session:
        from sqlalchemy import select

        try:
            query = select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()
        except Exception:
            raise HTTPException(status_code=404, detail="Source not found")

        if not source or source.user_id != parse_user_id(user_id):
            raise HTTPException(status_code=404, detail="Source not found")

        return {
            "id": str(source.id),
            "type": source.type,
            "name": source.name,
            "uri": source.uri,
            "status": source.status,
            "created_at": source.created_at.isoformat(),
            "updated_at": source.updated_at.isoformat(),
            "metadata": source.source_metadata or {},
        }


@router.delete("/api/v1/sources/{source_id}")
async def delete_source(source_id: str, http_request: Request):
    """Delete a data source."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    async with get_session() as session:
        try:
            source_uuid = uuid.UUID(source_id)
            source = await session.get(Source, source_uuid)
        except Exception:
            raise HTTPException(status_code=404, detail="Source not found")

        if not source:
            raise HTTPException(status_code=404, detail="Source not found")

        if source.user_id != parse_user_id(user_id):
            raise HTTPException(status_code=404, detail="Source not found")

        from app.infra.db.postgres import Repository
        from sqlalchemy import select

        repo_query = select(Repository).where(Repository.source_id == source_id)
        repo_result = await session.execute(repo_query)
        repo = repo_result.scalars().first()
        if repo:
            await session.delete(repo)

        await session.delete(source)
        await session.commit()

        from app.services.client import get_service_client
        client = get_service_client()
        import asyncio

        asyncio.create_task(client.delete_graph_group(source_id, user_id))

        return {"success": True, "message": "Source deleted successfully"}


@router.post("/api/v1/sources/{source_id}/sync")
async def sync_source_endpoint(source_id: str, background_tasks: BackgroundTasks):
    """Trigger a manual sync for a source."""
    return await start_ingestion(IngestRequest(source_id=source_id), background_tasks)


# =============================================================================
# External API Routes (Cloud Storage & URLs)
# =============================================================================


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
            # If onedrive fails, fallback to windowslive
            if provider == "onedrive" and e.status_code == 401:
                try:
                    tokens = await client.get_auth_token(user_id, "windowslive")
                except HTTPException:
                    raise e  # raise the original exception
            else:
                raise

        if not tokens or not tokens.get("access_token"):
            raise HTTPException(
                status_code=401, detail=f"No active connection found for {provider}"
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
            source = Source(
                user_id=parse_user_id(None),
                type="gdrive",
                name="Google Drive Sync",
                uri="gdrive://sync",
                source_metadata={
                    "folder_id": request.folder_id,
                    "include_patterns": request.include_patterns,
                    "exclude_patterns": request.exclude_patterns,
                },
            )
            session.add(source)
            await session.commit()
            await session.refresh(source)

            background_tasks.add_task(
                sync_gdrive_background,
                str(source.id),
                request.folder_id,
                request.include_patterns,
                request.exclude_patterns,
            )

            return {"source_id": str(source.id), "message": "Google Drive sync started"}
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
            source = Source(
                user_id=parse_user_id(None),
                type="dropbox",
                name="Dropbox Sync",
                uri="dropbox://sync",
                source_metadata={
                    "folder_path": request.folder_path,
                    "include_patterns": request.include_patterns,
                    "exclude_patterns": request.exclude_patterns,
                },
            )
            session.add(source)
            await session.commit()
            await session.refresh(source)

            background_tasks.add_task(
                sync_dropbox_background,
                str(source.id),
                request.folder_path,
                request.include_patterns,
                request.exclude_patterns,
            )

            return {"source_id": str(source.id), "message": "Dropbox sync started"}
    except Exception as e:
        logger.error("Dropbox sync error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Internal API Routes (Webhooks & Credentials)
# =============================================================================


async def process_repo_update_webhook(
    provider: str, repo_url: str, branch: str, old_commit: str, new_commit: str
):
    """Process repository update webhook by emitting REPO_UPDATED event."""
    from app.infra.db.postgres import get_session, Repository
    from sqlalchemy import select
    from app.infra.events.repository_events import get_repo_event_publisher
    from app.security.credentials import get_credential_storage
    from app.security.credentials import get_jwt_generator
    import uuid as uuid_lib

    try:
        normalized_url = repo_url.rstrip("/").rstrip(".git").lower()

        async with get_session() as session:
            result = await session.execute(select(Repository).where(Repository.url == repo_url))
            repo = result.scalars().first()

            if not repo:
                result = await session.execute(
                    select(Repository).where(Repository.url.ilike(f"%{normalized_url}%"))
                )
                repo = result.scalars().first()

            if not repo:
                logger.warning(
                    "Repository not found for webhook", repo_url=repo_url, provider=provider
                )
                return

            repo_id = str(repo.id)
            user_id = "system"

            access_token = None
            if repo.source_id:
                try:
                    source_uuid = uuid_lib.UUID(repo.source_id)
                    source_obj = await session.get(Source, source_uuid)
                    if source_obj and source_obj.source_metadata:
                        credentials = source_obj.source_metadata.get("credentials", {})
                        access_token = credentials.get("access_token")
                except Exception as e:
                    logger.warning(
                        "Failed to get credentials for webhook", repo_id=repo_id, error=str(e)
                    )

            if not access_token:
                logger.error(
                    "No credentials available for webhook", repo_id=repo_id, repo_url=repo_url
                )
                return

            credential_storage = get_credential_storage()
            jwt_generator = get_jwt_generator()

            stored = await credential_storage.store_credential(
                repo_id=repo_id, provider=provider, user_id=user_id, access_token=access_token
            )

            if not stored:
                logger.error("Failed to store credentials for webhook", repo_id=repo_id)
                return

            credential_ref = jwt_generator.generate_credential_ref(
                provider=provider, repo_id=repo_id, user_id=user_id
            )

            publisher = get_repo_event_publisher()
            if not publisher:
                logger.error("Repo event publisher not available", repo_id=repo_id)
                return

            correlation_id = str(uuid_lib.uuid4())

            success = publisher.publish_repo_updated(
                repo_id=repo_id,
                url=repo_url,
                branch=branch,
                provider=provider,
                old_commit=old_commit,
                new_commit=new_commit,
                credential_ref=credential_ref,
                update_type="push",
                correlation_id=correlation_id,
            )

            if success:
                logger.info(
                    "Published REPO_UPDATED event for webhook",
                    repo_id=repo_id,
                    old_commit=old_commit[:8],
                    new_commit=new_commit[:8],
                )
            else:
                logger.error("Failed to publish REPO_UPDATED event", repo_id=repo_id)

    except Exception as e:
        logger.error(
            "Failed to process webhook", provider=provider, repo_url=repo_url, error=str(e)
        )


@router.post("/api/v1/internal/webhooks/github")
async def github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_github_event: str = Header(..., alias="X-GitHub-Event"),
    x_hub_signature: str = Header(..., alias="X-Hub-Signature-256"),
):
    """Handle GitHub webhook events."""
    try:
        body = await request.body()
        if not webhook_handler.verify_signature(
            "github", body, x_hub_signature, settings.github_webhook_secret
        ):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        if x_github_event != "push":
            logger.info("Ignoring non-push GitHub event", event_type=x_github_event)
            return {"status": "ignored", "event_type": x_github_event}

        import json

        payload = json.loads(body.decode())
        commit_info = webhook_handler.extract_github_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from GitHub webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info
        background_tasks.add_task(
            process_repo_update_webhook, "github", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "event_type": x_github_event,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("GitHub webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/internal/webhooks/gitlab")
async def gitlab_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_gitlab_token: str = Header(..., alias="X-Gitlab-Token"),
):
    """Handle GitLab webhook events."""
    try:
        if x_gitlab_token != settings.gitlab_webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid webhook token")

        payload = await request.json()
        object_kind = payload.get("object_kind", "")
        if object_kind != "push":
            logger.info("Ignoring non-push GitLab event", object_kind=object_kind)
            return {"status": "ignored", "object_kind": object_kind}

        commit_info = webhook_handler.extract_gitlab_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from GitLab webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info

        background_tasks.add_task(
            process_repo_update_webhook, "gitlab", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "object_kind": object_kind,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("GitLab webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/internal/webhooks/bitbucket")
async def bitbucket_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hook_uuid: str = Header(..., alias="X-Hook-UUID"),
    x_event_key: str = Header(..., alias="X-Event-Key"),
):
    """Handle Bitbucket webhook events."""
    try:
        payload = await request.json()
        if x_event_key != "repo:push":
            logger.info("Ignoring non-push Bitbucket event", event_key=x_event_key)
            return {"status": "ignored", "event_key": x_event_key}

        commit_info = webhook_handler.extract_bitbucket_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from Bitbucket webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info
        background_tasks.add_task(
            process_repo_update_webhook, "bitbucket", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "event_key": x_event_key,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("Bitbucket webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


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
            query = select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()

            if not source:
                raise HTTPException(status_code=404, detail="Source not found")

            metadata = source.source_metadata or {}
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
    """Start ingesting a source."""
    async with get_session() as session:
        from sqlalchemy import select

        job_manager = get_job_manager()

        try:
            query = select(Source).where(Source.id == uuid.UUID(request.source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()
        except Exception:
            raise HTTPException(status_code=404, detail="Source not found")

        if not source:
            raise HTTPException(status_code=404, detail="Source not found")

        job = await job_manager.create_job(
            source_id=request.source_id,
            source_type=SourceType(source.type),
            metadata={"force_reprocess": request.force_reprocess},
        )

        background_tasks.add_task(process_source_background, job.id, request.source_id, source)

        return {"job_id": job.id, "status": job.status.value, "message": "Ingestion started"}


async def process_source_background(job_id: str, source_id: str, source_obj):
    """Background task to process a source."""
    job_manager = get_job_manager()
    try:
        await job_manager.update_job_status(job_id, JobStatus.PROCESSING)

        # We fetch the source again since it's an async session and SQLAlchemy objects
        # should not be passed across sessions
        async with get_session() as session:
            from sqlalchemy import select
            import uuid

            query = select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()
            if not source:
                logger.error(
                    "Source not found in background task", job_id=job_id, source_id=source_id
                )
                await job_manager.update_job_status(job_id, JobStatus.FAILED)
                return

            source_type = source.type
            uri = source.uri
            metadata = source.source_metadata or {}
            credentials = metadata.get("credentials", {})
            branch = metadata.get("branch") or "main"
            access_token = credentials.get("access_token")
            user_id = str(source.user_id)

        logger.info(
            "Processing source", job_id=job_id, source_id=source_id, source_type=source_type
        )

        if source_type in ["github", "gitlab", "bitbucket"]:
            from app.security.credentials import get_credential_storage
            from app.security.credentials import get_jwt_generator
            from app.infra.events.repository_events import get_repo_event_publisher

            credential_storage = get_credential_storage()
            jwt_generator = get_jwt_generator()

            if access_token:
                stored = await credential_storage.store_credential(
                    repo_id=source_id,
                    provider=source_type,
                    user_id=user_id,
                    access_token=access_token,
                )
                if not stored:
                    logger.error("Failed to store credentials for sync", repo_id=source_id)
                    await job_manager.update_job_status(job_id, JobStatus.FAILED)
                    return

            credential_ref = jwt_generator.generate_credential_ref(
                provider=source_type, repo_id=source_id, user_id=user_id
            )

            publisher = get_repo_event_publisher()
            if not publisher:
                logger.error("Repo event publisher not available", repo_id=source_id)
                await job_manager.update_job_status(job_id, JobStatus.FAILED)
                return

            success = publisher.publish_repo_ingest_requested(
                repo_id=source_id,
                url=uri,
                branch=branch,
                provider=source_type,
                commit_id="HEAD",
                credential_ref=credential_ref,
                user_id=user_id,
                correlation_id=job_id,
            )

            if success:
                logger.info("Published REPO_INGEST_REQUESTED event", repo_id=source_id)
                # The unified-processor or the consumer will mark it as COMPLETED
                # But for now, we'll mark it as COMPLETED to indicate we successfully dispatched it
                await job_manager.update_job_status(job_id, JobStatus.COMPLETED)
            else:
                logger.error("Failed to publish REPO_INGEST_REQUESTED event", repo_id=source_id)
                await job_manager.update_job_status(job_id, JobStatus.FAILED)

    except Exception as e:
        logger.error("Failed to process source", job_id=job_id, error=str(e))
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

            query = select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()

            if not source:
                logger.error(f"Source not found: {source_id}")
                return

            credentials = source.source_metadata.get("credentials", {})

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

        # Direct publishing of file content to Kafka/unified-processor has been
        # removed. Use the event-driven pipeline: create a Source and emit a
        # lightweight sync request so unified-processor pulls content itself.
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

            query = select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()

            if not source:
                logger.error(f"Source not found: {source_id}")
                return

            credentials = source.source_metadata.get("credentials", {})

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

        # Direct publishing of file content to Kafka/unified-processor has been
        # removed. Use the event-driven pipeline: create a Source and emit a
        # lightweight sync request so unified-processor pulls content itself.
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
