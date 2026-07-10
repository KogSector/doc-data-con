import structlog
from typing import Dict, Any
import uuid
from sqlalchemy import select as sa_select

from app.config import get_settings
from app.infra.db.postgres import get_session, Source
from app.services.documents.processor import get_document_processor
from app.services.client import get_service_client
from app.router import get_router
from app.models import FileType

logger = structlog.get_logger()
settings = get_settings()


async def trigger_repo_sync(source_id: str, source_type: str, metadata: Dict[str, Any] = None):
    logger.info(
        "[SYNC] Starting background initial sync for repo",
        source_id=source_id,
        source_type=source_type,
    )

    try:
        from app.infra.db.postgres import init_postgresql

        await init_postgresql()

        async with get_session() as session:
            query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()

            if not source:
                logger.error("[SYNC] Source not found in database", source_id=source_id)
                return

            uri = source.uri
            metadata = source.source_metadata or {}
            credentials = metadata.get("credentials")

        success = await sync_git_repo_api(source_id, source_type, uri, credentials, metadata)

        if not success:
            logger.error("[SYNC] Sync failed or yielded no results", source_id=source_id)
            return

        logger.info("[SYNC] === Sync stream completed ===", source_id=source_id)

    except Exception as e:
        logger.error(
            "[SYNC] Failed local sync download", source_id=source_id, error=str(e), exc_info=True
        )


async def sync_git_repo_api(
    source_id: str,
    provider: str,
    uri: str,
    credentials: Dict[str, str] | None,
    metadata: Dict[str, Any],
) -> bool:
    """Download git repository via API without cloning, and stream files."""
    logger.info(f"Starting API stream sync for {provider}", source_id=source_id, uri=uri)

    try:
        connector = None
        if provider == "github":
            from app.connectors.github_client import GitHubConnector

            connector = GitHubConnector(settings)
        elif provider == "gitlab":
            from app.connectors.gitlab_client import GitLabConnector

            connector = GitLabConnector(settings)
        elif provider == "bitbucket":
            from app.connectors.bitbucket_client import BitbucketConnector

            connector = BitbucketConnector(settings)

        if not connector:
            logger.error("Unsupported git provider", provider=provider)
            return False

        # Fetch repository to get branch
        from app.infra.db.postgres import Repository

        async with get_session() as session:
            repo_result = await session.execute(
                sa_select(Repository).where(Repository.source_id == source_id)
            )
            repo_record = repo_result.scalars().first()

        branch = repo_record.branch if repo_record else "main"

        # Parse the URI to get the owner/repo path (e.g., from https://github.com/mvp-2003/Proposal -> mvp-2003/Proposal)
        import urllib.parse

        parsed_uri = urllib.parse.urlparse(uri)
        repo_path = parsed_uri.path.strip("/")
        if repo_path.endswith(".git"):
            repo_path = repo_path[:-4]

        # Use fetch_source to get the tree and download files in memory
        provided_credentials = credentials or {}

        # Determine latest commit
        latest_commit = None
        try:
            if provider == "github":
                latest_commit = await connector.get_latest_commit(repo_path, branch)
            elif provider == "gitlab":
                latest_commit = await connector.get_latest_commit(repo_path, branch)
            elif provider == "bitbucket":
                parts = repo_path.split("/")
                if len(parts) >= 2:
                    latest_commit = await connector.get_latest_commit(parts[0], parts[1], branch)
        except Exception as e:
            logger.warning("Could not fetch latest commit", error=str(e))

        last_commit_hash = metadata.get("last_commit_hash")

        if last_commit_hash and latest_commit and last_commit_hash != latest_commit:
            logger.info(
                "Found previous sync, triggering incremental update",
                old=last_commit_hash,
                new=latest_commit,
            )
            from app.security.credentials import get_jwt_generator

            jwt_generator = get_jwt_generator()
            user_id_str = "system"
            async with get_session() as session:
                query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
                result = await session.execute(query)
                source = result.scalar_one_or_none()
                if source and source.user_id:
                    user_id_str = str(source.user_id)

            credential_ref = jwt_generator.generate_credential_ref(
                provider=provider, repo_id=source_id, user_id=user_id_str
            )

            publisher = None
            if publisher:
                success = False
                if success:
                    # Update metadata with new commit hash
                    try:
                        async with get_session() as session:
                            query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
                            update_result = await session.execute(query)
                            source_record = update_result.scalar_one_or_none()
                            if source_record:
                                new_metadata = dict(source_record.source_metadata or {})
                                new_metadata["last_commit_hash"] = latest_commit
                                source_record.source_metadata = new_metadata
                                await session.commit()
                                logger.info(
                                    "Updated last_commit_hash in metadata", source_id=source_id
                                )
                    except Exception as db_e:
                        logger.error("Failed to update last_commit_hash", error=str(db_e))
                    return True

        old_access_token = provided_credentials.get("access_token")

        files_processed, total_size = await connector.fetch_source(
            uri=repo_path,
            credentials=provided_credentials,
            branch=branch,
            include_patterns=metadata.get("include_patterns", ["**/*"]),
            exclude_patterns=metadata.get("exclude_patterns", []),
        )

        new_access_token = provided_credentials.get("access_token")
        if old_access_token and new_access_token and old_access_token != new_access_token:
            # Token was refreshed, update DB
            try:
                async with get_session() as session:
                    query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
                    update_result = await session.execute(query)
                    source_record = update_result.scalar_one_or_none()
                    if source_record:
                        new_metadata = dict(source_record.source_metadata or {})
                        new_metadata["credentials"] = provided_credentials
                        if latest_commit:
                            new_metadata["last_commit_hash"] = latest_commit
                        source_record.source_metadata = new_metadata
                        await session.commit()
                        logger.info(
                            "Updated source metadata with refreshed tokens and last_commit_hash",
                            source_id=source_id,
                        )
            except Exception as db_e:
                logger.error(
                    "Failed to update source metadata with refreshed token",
                    source_id=source_id,
                    error=str(db_e),
                )
        elif latest_commit:
            try:
                async with get_session() as session:
                    query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
                    update_result = await session.execute(query)
                    source_record = update_result.scalar_one_or_none()
                    if source_record:
                        new_metadata = dict(source_record.source_metadata or {})
                        new_metadata["last_commit_hash"] = latest_commit
                        source_record.source_metadata = new_metadata
                        await session.commit()
                        logger.info(
                            "Updated source metadata with last_commit_hash", source_id=source_id
                        )
            except Exception as db_e:
                logger.error(
                    "Failed to update source metadata with last_commit_hash",
                    source_id=source_id,
                    error=str(db_e),
                )

        logger.info(f"Streaming {len(files_processed)} files from {provider}", source_id=source_id)

        user_id_str = "system"
        async with get_session() as session:
            query = sa_select(Source).where(Source.id == uuid.UUID(source_id))
            result = await session.execute(query)
            source = result.scalar_one_or_none()
            if source and source.user_id:
                user_id_str = str(source.user_id)

        router = get_router()
        client = get_service_client()

        for file_info in files_processed:
            try:
                clean_path = (
                    file_info.get("path", "") or file_info.get("name", "unknown.file")
                ).lstrip("/")
                content = file_info.get("content")

                if content:
                    import os
                    _, ext = os.path.splitext(clean_path.lower())
                    binary_exts = {
                        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".pdf",
                        ".zip", ".tar", ".gz", ".7z", ".rar",
                        ".exe", ".dll", ".so", ".dylib", ".bin", ".whl",
                        ".mp4", ".mp3", ".wav",
                        ".ttf", ".woff", ".woff2", ".eot", ".svg", ".webp", ".tiff", ".otf"
                    }
                    if ext in binary_exts:
                        continue

                    source_id_str = str(repo_record.id) if repo_record else source_id
                    file_type = router.detect_file_type(clean_path)

                    if file_type == FileType.CODE:
                        payload = {
                            "content": content if isinstance(content, str) else content.decode("utf-8", errors="replace"),
                            "filename": clean_path,
                            "source_id": source_id_str,
                            "user_id": user_id_str,
                            "is_base64": False
                        }
                        await client.send_to_processor_http(
                            endpoint="/api/v1/process", payload=payload, timeout=60.0
                        )
                    else:
                        await get_document_processor().process_document(
                            source_id=source_id_str,
                            file_id=str(uuid.uuid4()),
                            filename=clean_path,
                            content=content if isinstance(content, bytes) else content.encode("utf-8"),
                            metadata={"provider": provider, "url": uri},
                            user_id=user_id_str,
                        )

            except Exception as e:
                logger.error("Failed to stream generic file", provider=provider, error=str(e))

        return True

    except Exception as e:
        logger.error("Git repo API sync failed", provider=provider, error=str(e))
        return False
