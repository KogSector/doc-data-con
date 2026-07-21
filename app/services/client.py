"""
Data Connector Service - Downstream Service Client
Handles communication with unified-processor via Kafka events.
Uses gRPC for internal service-to-service communication (auth, health checks).
"""

import structlog
from typing import Any
from fastapi import HTTPException

from app.config import get_settings
from app.security.middleware_auth import _default_auth
from enum import Enum

class SourceType(str, Enum):
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    URL = "url"
    GOOGLE_DRIVE = "google_drive"
    FILE_UPLOAD = "file_upload"
    WEB = "web"
    UNKNOWN = "unknown"

logger = structlog.get_logger()


class ServiceClient:
    """Client for communicating with downstream processing services via Kafka events."""

    def __init__(self):
        self.settings = get_settings()

    async def trigger_source_sync(
        self,
        source_id: str,
        source_type: str,
        source_url: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Trigger source sync via Kafka event (Decoupled).

        Args:
            source_id: ID of the source being processed
            source_type: Type of source (github, url, gdrive, etc.)
            source_url: URL or URI of the source
            metadata: Optional metadata (user_id, tenant_id, etc.)
        """
        try:
            # Map string to SourceType enum
            try:
                stype = SourceType(source_type.lower())
            except ValueError:
                # Map common variations
                mapping = {
                    "google_drive": SourceType.GOOGLE_DRIVE,
                    "file_upload": SourceType.FILE_UPLOAD,
                    "web": SourceType.WEB,
                    "url": SourceType.URL,
                }
                stype = mapping.get(source_type.lower(), SourceType.UNKNOWN)

            producer = None

            if stype == SourceType.URL:
                logger.info("Triggering direct URL scrape", url=source_url, source_id=source_id)
                await self.send_to_processor_http(
                    "/api/v1/web/scrape",
                    {"source_id": source_id, "url": source_url, "metadata": metadata or {}},
                )
                return

            logger.warning(
                "Source sync requests via Kafka are deprecated. Ignoring non-URL sync request.",
                source_id=source_id,
                source_type=source_type,
            )
        except Exception as e:
            logger.error("Failed to publish source sync request", error=str(e), source_id=source_id)
            raise

    # Deprecated methods for direct content ingestion were removed in favor
    # of the event-driven pipeline. Use `trigger_source_sync` and
    # `send_to_processor_http` for supported flows.

    async def check_service_health(self, service: str) -> bool:
        """
        Check if a downstream service is healthy.

        Note: This method uses direct HTTP health checks for monitoring purposes.
        Health checks are acceptable for observability and are not part of the
        data ingestion pipeline.

        Args:
            service: Name of the service to check

        Returns:
            True if healthy, False otherwise
        """
        # Import httpx only when needed for health checks
        try:
            import httpx
        except ImportError:
            logger.warning("httpx not available for health checks")
            return False

        url_map = {
            "doc-uni-proc": self.settings.doc_uni_proc_url,
        }

        base_url = url_map.get(service)
        if not base_url:
            return False

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{base_url}/health")
                return response.status_code == 200
        except Exception:
            return False

    async def send_to_processor_http(
        self,
        endpoint: str,
        payload: dict,
        timeout: float = 60.0,
        headers: dict | None = None,
    ) -> dict:
        """
        Forward data to unified-processor via internal HTTP POST.

        This is used for CRM/GRC sync flows where the data-connector fetches
        records from third-party APIs and sends the structured payload to
        unified-processor for chunking and embedding.

        Unlike the deprecated ``send_to_unified_processor`` (which embedded file
        content in Kafka messages), this is a targeted control-plane call for
        already-structured application data.

        Args:
            endpoint: Relative endpoint path, e.g. "/api/v1/crms/process"
            payload: JSON-serialisable dictionary
            timeout: HTTP timeout in seconds (default 60s)
            headers: Optional dictionary of HTTP headers to include

        Returns:
            Parsed JSON response from unified-processor

        Raises:
            HTTPException: On connection or HTTP errors
        """
        try:
            import httpx
        except ImportError:
            logger.error("httpx not available for HTTP forwarding")
            raise HTTPException(
                status_code=500, detail="httpx dependency not available for internal HTTP call"
            )

        base_url = self.settings.doc_uni_proc_url
        url = f"{base_url}{endpoint}"

        logger.info(
            "[SERVICE-CLIENT] Forwarding data to unified-processor",
            url=url,
            payload_keys=list(payload.keys()) if isinstance(payload, dict) else "non-dict",
        )

        try:
            import asyncio
            req_headers = {"X-API-Key": self.settings.internal_api_key}
            if headers:
                req_headers.update(headers)
            retry_attempts = getattr(self.settings, 'doc_uni_proc_retry_attempts', 10)

            for attempt in range(retry_attempts):
                try:
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        response = await client.post(url, json=payload, headers=req_headers)
                        
                        if response.status_code >= 500:
                            if attempt < retry_attempts - 1:
                                wait_time = min(30, 2 ** attempt)
                                logger.warning(
                                    f"[SERVICE-CLIENT] Server error {response.status_code}, retrying in {wait_time}s (attempt {attempt + 1}/{retry_attempts})",
                                )
                                await asyncio.sleep(wait_time)
                                continue

                        if response.status_code >= 400:
                            error_body = response.text
                            if len(error_body) > 200:
                                error_body = error_body[:200] + "... [truncated]"
                            logger.error(
                                "[SERVICE-CLIENT] Unified-processor returned error",
                                status_code=response.status_code,
                                body=error_body,
                            )
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=f"Unified-processor error: {error_body}",
                            )
                        return response.json()
                except (httpx.RequestError, httpx.HTTPError) as e:
                    if attempt < retry_attempts - 1:
                        wait_time = min(30, 2 ** attempt)
                        logger.warning(
                            f"[SERVICE-CLIENT] Request/Protocol error ({str(e)}), retrying in {wait_time}s (attempt {attempt + 1}/{retry_attempts})",
                            error=str(e)
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error("[SERVICE-CLIENT] Cannot reach unified-processor after retries", url=url, error=str(e))
                    raise HTTPException(
                        status_code=503, detail=f"Unified-processor unreachable at {base_url}: {str(e)}"
                    )
        except Exception as e:
            if not isinstance(e, HTTPException):
                logger.error("[SERVICE-CLIENT] Unexpected error", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))
            raise

    async def delete_graph_group(self, group_id: str, user_id: str) -> bool:
        """
        Directly connect to FalkorDB and Postgres to delete all data associated with a given group_id (source_id).
        This performs a highly efficient tag-based cascading delete using DETACH DELETE, and clears PG metadata.
        """
        import redis.asyncio as redis
        from sqlalchemy import text
        from app.infra.db.postgres import get_session

        settings = self.settings

        # 1. Delete from Postgres
        try:
            logger.info("Deleting postgres data for source", source_id=group_id)
            async with get_session() as session:
                await session.execute(text("DELETE FROM file_metadata WHERE source_id = :source_id"), {"source_id": group_id})
                await session.execute(text("DELETE FROM processing_jobs WHERE source_id = :source_id"), {"source_id": group_id})
                await session.execute(text("DELETE FROM chunk_snapshot WHERE source_id = :source_id"), {"source_id": group_id})
                await session.commit()
        except Exception as e:
            logger.error("Failed to delete postgres data for source", source_id=group_id, error=str(e))

        if not settings.falkordb_host:
            logger.error("FalkorDB host not configured")
            return False

        client = None
        try:
            logger.info(
                "Connecting to FalkorDB for direct graph group deletion",
                group_id=group_id,
                host=settings.falkordb_host,
            )
            client = redis.Redis(
                host=settings.falkordb_host,
                port=settings.falkordb_port,
                username=settings.falkordb_username,
                password=settings.falkordb_password,
                decode_responses=True,
                ssl=False,
            )

            user_id_esc = user_id.replace('"', '\\"')
            
            logger.info("Executing Cypher query for fast label-based deletion in FalkorDB", group_id=group_id, graph_name=f"graph-{user_id}")
            
            # Explicitly specify labels to utilize indices and avoid full graph scans
            labels_to_clean = ["Vector_Chunk", "Code_Entity", "Web_Page", "Repository", "File", "Class", "Function", "Directory", "Project"]
            total_deleted = 0
            
            for label in labels_to_clean:
                if label == "Repository":
                    cypher = f"""
                    MATCH (n:Repository)
                    WHERE n.id = "{group_id}" AND n.owner_id = "{user_id_esc}"
                    WITH collect(n) AS nodes, count(n) AS count
                    UNWIND nodes AS n
                    DETACH DELETE n
                    RETURN count AS total
                    """
                else:
                    cypher = f"""
                    MATCH (n:{label})
                    WHERE n.source_id = "{group_id}" AND n.owner_id = "{user_id_esc}"
                    WITH collect(n) AS nodes, count(n) AS count
                    UNWIND nodes AS n
                    DETACH DELETE n
                    RETURN count AS total
                    """
                
                result = await client.execute_command("GRAPH.QUERY", f"graph-{user_id}", cypher)
                
                # Extract count from result safely
                try:
                    if result and len(result) > 1 and len(result[1]) > 0 and len(result[1][0]) > 0:
                        total_deleted += int(result[1][0][0])
                except (IndexError, ValueError, TypeError):
                    pass

            logger.info(
                "Successfully executed direct graph group deletion",
                group_id=group_id,
                total_deleted=total_deleted,
            )
            return True

        except Exception as e:
            logger.error("Error executing direct graph group deletion", group_id=group_id, error=str(e))
            return False
        finally:
            if client is not None:
                await client.aclose()
    async def get_auth_token(self, user_id: str, provider: str) -> dict[str, Any]:
        """
        Get auth token from auth-middleware (Internal API, prefers gRPC).
        """
        logger.info(
            "[AUTH-TOKEN] Requesting token from auth service", user_id=user_id, provider=provider
        )
        try:
            result = await _default_auth.get_internal_token(
                api_key=self.settings.internal_api_key, user_id=user_id, provider=provider
            )

            logger.info(
                "[AUTH-TOKEN] Received response from auth service",
                user_id=user_id,
                requested_provider=provider,
                returned_provider=result.get("provider"),
                success=result.get("success"),
            )

            if result.get("success"):
                return result
            else:
                error_msg = result.get("error", "Unknown error")
                logger.error(
                    "Failed to fetch auth token via gRPC",
                    user_id=user_id,
                    provider=provider,
                    error=error_msg,
                )
                # Distinguish between "no connection exists" and "token refresh failed"
                status_code = 404 if "not found" in error_msg.lower() or "invalid" in error_msg.lower() else 401
                raise HTTPException(
                    status_code=status_code,
                    detail=f"Failed to fetch credentials for {provider}. Details: {error_msg}",
                )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during auth token retrieval",
                user_id=user_id,
                provider=provider,
                error=str(e),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch credentials for {provider}. Error: {str(e)}",
            )


# Global client instance
_service_client: ServiceClient | None = None


def get_service_client() -> ServiceClient:
    """Get the global service client instance."""
    global _service_client
    if _service_client is None:
        _service_client = ServiceClient()
    return _service_client
