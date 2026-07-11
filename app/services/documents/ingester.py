"""
Local Downloader - Downloads connected cloud storage files and git repos directly to local disk
"""

import structlog
from pathlib import Path
from typing import Dict, Any

from app.config import get_settings
from app.infra.db.postgres import get_session, Source
from app.services.documents.processor import get_document_processor
import uuid

# Import sa_select for SQLAlchemy
from sqlalchemy import select as sa_select
from sqlalchemy.orm.attributes import flag_modified

async def _update_source_metadata_files(source_id: str, uploaded_files_info: list):
    if not uploaded_files_info:
        return
    try:
        async with get_session() as session:
            query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source_record = result.scalar_one_or_none()
            if source_record:
                new_metadata = dict(source_record.source_metadata or {})
                new_metadata["files"] = uploaded_files_info
                new_metadata["total_size_bytes"] = sum(f["size_bytes"] for f in uploaded_files_info)
                source_record.source_metadata = new_metadata
                flag_modified(source_record, "source_metadata")
                await session.commit()
                logger.info(f"Updated source {source_id} metadata with {len(uploaded_files_info)} files")
    except Exception as e:
        logger.error(f"Failed to update source metadata for {source_id}", error=str(e))

logger = structlog.get_logger()
settings = get_settings()
DOWNLOADS_DIR = Path(settings.downloads_folder)


async def trigger_initial_sync(source_id: str, source_type: str, source_metadata: Dict[str, Any]):
    """Background task to initially sync files and repos locally"""
    logger.info(
        "[SYNC] === Starting local sync download ===",
        source_id=source_id,
        source_type=source_type,
        downloads_dir=str(DOWNLOADS_DIR),
    )

    try:
        # Initialize PostgreSQL for this background task
        from app.infra.db.postgres import init_postgresql

        await init_postgresql()

        # Create base downloads directory if it doesn't exist
        # DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("[SYNC] Downloads directory creation is disabled", path=str(DOWNLOADS_DIR))

        # Fetch the complete source record from DB to get the reliable URI
        async with get_session() as session:
            query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()

            if not source:
                logger.error("[SYNC] Source not found in database", source_id=source_id)
                return

            user_id = str(source.user_id) if source.user_id else "system"

            uri = source.uri
            metadata = source.source_metadata or {}
            credentials = metadata.get("credentials")
            logger.info(
                "[SYNC] Source fetched from DB",
                source_id=source_id,
                uri=uri,
                has_credentials=bool(credentials),
            )

        success = False
        if source_type in ["google-drive", "google_drive", "google"]:
            success = await sync_google_drive_local(source_id, credentials, metadata, user_id=user_id)
        elif source_type == "upload":
            success = await sync_uploaded_files(source_id, uri, metadata, user_id=user_id)

        else:
            # Generic downloader for all other supported sources
            success = await sync_generic_source_local(
                source_id, source.type, uri, credentials, metadata, user_id=user_id
            )

        if not success:
            logger.error("[SYNC] Sync failed or yielded no results", source_id=source_id)
            return

        logger.info("[SYNC] === Sync stream completed ===", source_id=source_id)

    except Exception as e:
        logger.error(
            "[SYNC] Failed local sync download", source_id=source_id, error=str(e), exc_info=True
        )


async def sync_google_drive_local(
    source_id: str, credentials: Dict[str, str], metadata: Dict[str, Any], user_id: str = "system"
) -> bool:
    """Download Google Drive files and stream them"""
    from app.connectors.gdrive_client import GoogleDriveConnector

    if not credentials or not credentials.get("access_token"):
        logger.error("No access token provided for Google Drive local sync", source_id=source_id)
        return False

    try:
        connector = GoogleDriveConnector(settings)
        connector.set_credentials(
            access_token=credentials.get("access_token"),
            refresh_token=credentials.get("refresh_token"),
        )

        folder_id = metadata.get("folder_id")

        # List files
        files_data = await connector.list_files(folder_id=folder_id)
        files = files_data.get("files", [])

        # Only download supported types
        supported_files = [f for f in files if f.get("supported", False)]

        logger.info(
            f"Downloading {len(supported_files)} files from Google Drive locally",
            source_id=source_id,
        )

        uploaded_files_info = []
        for file_info in supported_files:
            uploaded_files_info.append({
                "name": file_info.get("name", "unknown"),
                "size_bytes": int(file_info.get("size", 0) or 0),
                "content_type": file_info.get("mimeType", "application/octet-stream")
            })
        await _update_source_metadata_files(source_id, uploaded_files_info)

        for file_info in supported_files:
            try:
                # Download
                file_data = await connector.download_file(file_info["id"])

                # Stream file
                content = file_data["content"]
                if content:
                    await get_document_processor().process_document(
                        source_id=source_id,
                        file_id=file_info["id"],
                        filename=file_data["filename"],
                        content=content if isinstance(content, bytes) else content.encode("utf-8"),
                        metadata={"provider": "google-drive"},
                        user_id=user_id,
                    )

                logger.info(f"Successfully streamed {file_data['filename']}")

            except Exception as e:
                logger.error(f"Failed to stream GDrive file {file_info.get('name')}", error=str(e))
                
        return True

    except Exception as e:
        logger.error("Google Drive sync stream failed entirely", error=str(e))
        return False


# Local repository cloning is deprecated and handled by unified-processor.
# The previous `sync_git_repo_local` function has been removed.


async def sync_generic_source_local(
    source_id: str,
    provider: str,
    uri: str,
    credentials: Dict[str, str] | None,
    metadata: Dict[str, Any],
    user_id: str = "system",
) -> bool:
    """Download generic data sources and stream them"""
    logger.info(f"Starting generic stream sync for {provider}", source_id=source_id, uri=uri)

    try:
        # Route to appropriate connector based on provider
        connector = None

        # Only loading specific connectors based on the type
        if provider == "onedrive":
            from app.connectors.onedrive_client import OneDriveConnector

            connector = OneDriveConnector(settings)
        elif provider == "dropbox":
            from app.connectors.dropbox_client import DropboxConnector

            connector = DropboxConnector(settings)
        elif provider == "notion":
            from app.connectors.notion_client import NotionConnector

            connector = NotionConnector(settings)
        # Add additional connector mappings as supported

        if not connector:
            logger.info(
                "Local sync currently unsupported/skipped generically for provider",
                provider=provider,
            )
            return False

        # Use fetch command just to get the raw documents
        files_processed, total_size = await connector.fetch_source(
            uri=uri,
            credentials=credentials or {},
            include_patterns=metadata.get("include_patterns", ["**/*"]),
            exclude_patterns=metadata.get("exclude_patterns", []),
            source_metadata=metadata,
        )

        logger.info(f"Streaming {len(files_processed)} files from {provider}", source_id=source_id)
        
        uploaded_files_info = []
        for file_info in files_processed:
            clean_path = (file_info.get("path", "") or file_info.get("name", "unknown.file")).lstrip("/")
            content = file_info.get("content")
            uploaded_files_info.append({
                "name": clean_path,
                "size_bytes": len(content) if content else 0,
                "content_type": file_info.get("mime_type", "application/octet-stream")
            })
        
        await _update_source_metadata_files(source_id, uploaded_files_info)

        for file_info in files_processed:
            try:
                # the array object usually has 'path', 'content', 'name'
                clean_path = (
                    file_info.get("path", "") or file_info.get("name", "unknown.file")
                ).lstrip("/")

                content = file_info.get("content")
                if content:
                    await get_document_processor().process_document(
                        source_id=source_id,
                        file_id=str(uuid.uuid4()),
                        filename=clean_path,
                        content=content if isinstance(content, bytes) else content.encode("utf-8"),
                        metadata={"provider": provider},
                        user_id=user_id,
                    )

                logger.debug("Successfully streamed generic file", path=clean_path)

            except Exception as e:
                logger.error("Failed to stream generic file", provider=provider, error=str(e))
                
        return True

    except Exception as e:
        logger.error("Generic stream sync failed", provider=provider, error=str(e))
        return False


async def sync_uploaded_files(source_id: str, uri: str, metadata: Dict[str, Any], user_id: str = "system") -> bool:
    """Stream uploaded files from the local shared directory to Kafka."""
    logger.info("Starting stream for uploaded files", source_id=source_id, uri=uri)

    try:
        # uri is like local://docs/{source_id}
        if not uri.startswith("local://"):
            return False

        rel_path = uri.replace("local://", "")
        target_dir = DOWNLOADS_DIR / rel_path

        if not target_dir.exists() or not target_dir.is_dir():
            logger.error("Upload directory not found", directory=str(target_dir))
            return False

        try:
            from app.services.client import get_service_client

            client = get_service_client()

            # Use the passed user_id or fallback
            user_id_str = user_id
            if user_id_str == "system":
                async with get_session() as session:
                    query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
                    result = await session.execute(query)
                    source = result.scalar_one_or_none()
                    if source and source.user_id:
                        user_id_str = str(source.user_id)

            import base64
            import asyncio
            
            # Eagerly update metadata so frontend sees the files immediately
            uploaded_files_info = []
            for file_path in target_dir.iterdir():
                if file_path.is_file():
                    uploaded_files_info.append({
                        "name": file_path.name,
                        "size_bytes": file_path.stat().st_size,
                        "content_type": "application/octet-stream"
                    })
            await _update_source_metadata_files(source_id, uploaded_files_info)
            
            # Send each file in the target directory to the unified-processor
            tasks = []
            for file_path in target_dir.iterdir():
                if file_path.is_file():
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                        
                    b64_content = base64.b64encode(file_content).decode("utf-8")
                    
                    logger.info(f"Sending base64 encoded document to unified-processor for {file_path.name}")
                    
                    tasks.append(
                        client.send_to_processor_http(
                            endpoint="/api/v1/documents/process",
                            payload={
                                "content": b64_content,
                                "is_base64": True,
                                "filename": file_path.name,
                                "source_id": source_id,
                                "user_id": user_id_str,
                            },
                            timeout=300.0,
                        )
                    )
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = 0
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error("Failed to process file", error=str(result))
                    else:
                        success_count += 1
                        
                if success_count == 0:
                    logger.error("Failed to trigger unified-processor for any uploaded files")
                    return False
                else:
                    logger.info(f"Successfully triggered unified-processor for {success_count}/{len(tasks)} uploaded files")

            # Clean up the temporary directory after processing
            if target_dir.exists():
                import shutil
                shutil.rmtree(target_dir)
                logger.info("Cleaned up temporary document directory", directory=str(target_dir))

            return True

        except Exception as e:
            logger.error("Failed to trigger unified-processor processing", error=str(e))
            # Ensure cleanup on failure as well
            if target_dir.exists():
                import shutil
                try:
                    shutil.rmtree(target_dir)
                except Exception as cleanup_err:
                    logger.warning("Failed to clean up target_dir", error=str(cleanup_err))
            return False

    except Exception as e:
        logger.error("Upload sync failed", error=str(e))
        return False
