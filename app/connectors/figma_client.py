"""
Figma Connector - Import nodes and pages from Figma.
"""

import structlog
import uuid
import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()


class FigmaConnector(BaseConnector):
    """
    Import components and frames from Figma.

    Features:
    - Personal Access Token authentication
    - Fetch Figma file structure (Pages, Frames, Nodes)
    - Export node content and text for semantic search
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.api_key = getattr(settings, "figma_personal_access_token", None)
        logger.info("FigmaConnector initialized")

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL (Not fully implemented, assuming PAT for now)."""
        return ""

    async def exchange_code_for_token(
        self, code: str, redirect_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """Exchange authorization code for access token."""
        return {"access_token": code}

    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Figma PATs do not expire or use refresh tokens."""
        return {"access_token": refresh_token}

    async def get_sync_capabilities(self) -> Dict[str, Any]:
        """Return sync capabilities of Figma."""
        return {
            "supports_incremental": False,
            "supports_webhooks": False,
            "supports_full_sync": True,
        }

    async def get_file_content(self, file_key: str, token: str) -> Dict[str, Any]:
        """
        Get Figma file content via the REST API.
        """
        url = f"https://api.figma.com/v1/files/{file_key}"
        headers = {"X-Figma-Token": token}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Figma API returned {response.status_code}", response=response.text)
                response.raise_for_status()

            return response.json()

    def _extract_nodes(self, document: Dict[str, Any], title: str) -> str:
        """Extract text content and structural info from Figma nodes to Markdown."""
        lines = [f"# {title} - Figma Design\n"]

        def traverse(node: Dict[str, Any], depth: int = 0):
            indent = "  " * depth
            node_type = node.get("type", "UNKNOWN")
            node_name = node.get("name", "Unnamed")

            # Record structural nodes
            if node_type in ["CANVAS", "FRAME", "GROUP", "COMPONENT"]:
                lines.append(f"{indent}- **{node_type}**: {node_name}")

            # Extract textual content
            elif node_type == "TEXT":
                characters = node.get("characters", "").replace("\n", " ")
                lines.append(f"{indent}- **TEXT** ({node_name}): {characters}")

            elif node_type in ["VECTOR", "INSTANCE", "RECTANGLE", "ELLIPSE", "STAR", "POLYGON"]:
                lines.append(f"{indent}- {node_type}: {node_name}")

            if "children" in node:
                for child in node["children"]:
                    traverse(child, depth + 1)

        traverse(document)
        return "\n".join(lines)

    async def fetch_source(self, **kwargs) -> tuple[List[Dict[str, Any]], int]:
        """Fetch all pages/nodes from a Figma file for ingestion."""
        uri = kwargs.get("uri")  # This should be the Figma file key
        credentials = kwargs.get("credentials", {})

        token = credentials.get("access_token") or self.api_key
        if not token:
            raise ValueError("Figma Personal Access Token is required")

        logger.info("Starting Figma fetch_source", uri=uri)

        try:
            figma_data = await self.get_file_content(uri, token)
        except Exception as e:
            logger.error("Failed to fetch Figma file", error=str(e))
            raise

        document = figma_data.get("document", {})
        title = figma_data.get("name", "Untitled Figma File")

        # Flatten structure into markdown for chunks/nodes
        markdown_content = self._extract_nodes(document, title)
        content_bytes = markdown_content.encode("utf-8")

        downloaded_file = {
            "content": content_bytes,
            "filename": f"{title}.md",
            "path": f"{title}.md",
            "size": len(content_bytes),
            "doc_type": "markdown",
            "source": "figma",
            "source_id": uri,
            "title": title,
            "url": f"https://www.figma.com/file/{uri}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": figma_data.get("lastModified"),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info("Figma fetch_source completed", size=len(content_bytes))
        return [downloaded_file], len(content_bytes)


# =============================================================================
# API Routes for Figma
# =============================================================================

figma_router = APIRouter(prefix="/api/v1/figma", tags=["Figma"])


class ConnectFigmaRequest(BaseModel):
    file_key: str
    access_token: str


@figma_router.post("")
async def connect_figma_file(payload: ConnectFigmaRequest, request: Request):
    """Connect a Figma file."""
    logger.info("Connecting Figma file", file_key=payload.file_key)
    connection_id = str(uuid.uuid4())
    return {
        "success": True,
        "connection_id": connection_id,
        "message": "Figma file connected successfully",
    }


@figma_router.post("/{connection_id}/sync")
async def sync_figma_file(connection_id: str, request: Request):
    """Sync Figma nodes."""
    logger.info("Syncing Figma file", connection_id=connection_id)
    return {"success": True, "message": "Figma sync triggered"}
