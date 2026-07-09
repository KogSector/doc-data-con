"""
Notion Connector - Import pages from Notion.
"""

import fnmatch
import structlog
import uuid
from typing import Optional
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()

# Notion API imports - optional
NOTION_AVAILABLE = False
try:
    from notion_client import Client as NotionClient

    NOTION_AVAILABLE = True
except ImportError:
    logger.warning("Notion client not available. Install: pip install notion-client")


class NotionConnector(BaseConnector):
    """
    Import pages and databases from Notion.

    Features:
    - API key authentication
    - List databases and pages
    - Export pages as Markdown
    - Handle nested blocks and rich content
    """

    def __init__(self, settings: Settings):
        BaseConnector.__init__(self, settings)
        if not NOTION_AVAILABLE:
            raise ImportError("Notion client not available. Install: pip install notion-client")

        self.client_id = settings.notion_client_id
        self.client_secret = settings.notion_client_secret
        self.redirect_uri = settings.notion_redirect_uri
        self.api_key = settings.notion_api_key

        if self.api_key:
            self._client = NotionClient(auth=self.api_key)
        else:
            self._client = None

        logger.info("NotionConnector initialized")

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        BaseConnector.set_credentials(self, access_token, refresh_token)
        self._client = NotionClient(auth=access_token)

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        if not self.client_id:
            raise ValueError("NOTION_CLIENT_ID not configured")

        import urllib.parse

        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "owner": "user",
        }

        # Use provided redirect_uri or fall back to settings
        r_uri = redirect_uri or self.redirect_uri
        if r_uri:
            params["redirect_uri"] = r_uri
        if state:
            params["state"] = state

        return f"https://api.notion.com/v1/oauth/authorize?{urllib.parse.urlencode(params)}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """Exchange authorization code for access token."""
        import requests
        from requests.auth import HTTPBasicAuth

        if not self.client_id or not self.client_secret:
            raise ValueError("NOTION_CLIENT_ID or NOTION_CLIENT_SECRET not configured")

        headers = {
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
        data = {
            "grant_type": "authorization_code",
            "code": code,
        }

        r_uri = redirect_uri or self.redirect_uri
        if r_uri:
            data["redirect_uri"] = r_uri

        try:
            response = requests.post(
                "https://api.notion.com/v1/oauth/token",
                json=data,
                headers=headers,
                auth=HTTPBasicAuth(self.client_id, self.client_secret),
            )
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            if access_token:
                self.set_credentials(access_token)

            return {
                "access_token": access_token,
                "bot_id": token_data.get("bot_id"),
                "workspace_name": token_data.get("workspace_name"),
                "workspace_icon": token_data.get("workspace_icon"),
                "workspace_id": token_data.get("workspace_id"),
                "owner": token_data.get("owner"),
            }
        except Exception as e:
            logger.error("Notion token exchange failed", error=str(e))
            raise

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """Notion access tokens do not expire; return the current token."""
        return {"access_token": refresh_token}

    async def get_sync_capabilities(self) -> dict:
        """Return sync capabilities of Notion."""
        return {
            "supports_incremental": False,
            "supports_webhooks": False,
            "supports_full_sync": True,
        }

    async def list_databases(self) -> list[dict]:
        """List all accessible databases."""
        results = self._client.search(filter={"property": "object", "value": "database"}).get(
            "results", []
        )

        return [
            {
                "id": db["id"],
                "title": self._get_title(db),
                "url": db.get("url"),
                "created_at": db.get("created_time"),
                "updated_at": db.get("last_edited_time"),
            }
            for db in results
        ]

    async def list_pages(self, database_id: Optional[str] = None) -> list[dict]:
        """List pages, optionally from a specific database."""
        if database_id:
            results = self._client.databases.query(database_id=database_id).get("results", [])
        else:
            results = self._client.search(filter={"property": "object", "value": "page"}).get(
                "results", []
            )

        return [
            {
                "id": page["id"],
                "title": self._get_title(page),
                "url": page.get("url"),
                "created_at": page.get("created_time"),
                "updated_at": page.get("last_edited_time"),
                "parent_type": page.get("parent", {}).get("type"),
            }
            for page in results
        ]

    async def get_page_content(self, page_id: str) -> dict:
        """
        Get page content as Markdown.

        Fetches all blocks and converts to Markdown format.
        """
        # Get page metadata
        page = self._client.pages.retrieve(page_id=page_id)
        title = self._get_title(page)

        # Get all blocks
        blocks = self._get_all_blocks(page_id)

        # Convert to Markdown
        markdown = self._blocks_to_markdown(blocks, title)

        logger.info(
            "Exported Notion page",
            page_id=page_id,
            title=title,
            blocks=len(blocks),
        )

        content_bytes = markdown.encode("utf-8")
        return {
            "content": content_bytes,
            "filename": f"{title}.md",
            "size": len(content_bytes),
            "doc_type": "markdown",
            "source": "notion",
            "source_id": page_id,
            "title": title,
            "url": page.get("url"),
            "created_at": page.get("created_time"),
            "updated_at": page.get("last_edited_time"),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    def _get_all_blocks(self, block_id: str, depth: int = 0) -> list[dict]:
        """Recursively get all blocks."""
        if depth > 5:  # Limit nesting depth
            return []

        blocks = []
        cursor = None

        while True:
            response = self._client.blocks.children.list(
                block_id=block_id,
                start_cursor=cursor,
            )

            for block in response.get("results", []):
                block["_depth"] = depth
                blocks.append(block)

                # Recursively get children
                if block.get("has_children"):
                    children = self._get_all_blocks(block["id"], depth + 1)
                    blocks.extend(children)

            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

        return blocks

    def _blocks_to_markdown(self, blocks: list[dict], title: str) -> str:
        """Convert Notion blocks to Markdown."""
        lines = [f"# {title}\n"]

        for block in blocks:
            block_type = block.get("type", "")
            depth = block.get("_depth", 0)
            indent = "  " * depth

            content = self._extract_block_content(block, block_type)
            if content:
                lines.append(f"{indent}{content}")

        return "\n".join(lines)

    def _extract_block_content(self, block: dict, block_type: str) -> str:
        """Extract content from a single block."""
        block_data = block.get(block_type, {})

        if block_type == "paragraph":
            return self._rich_text_to_string(block_data.get("rich_text", []))

        elif block_type in ("heading_1", "heading_2", "heading_3"):
            level = int(block_type[-1])
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            return f"{'#' * level} {text}"

        elif block_type == "bulleted_list_item":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            return f"- {text}"

        elif block_type == "numbered_list_item":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            return f"1. {text}"

        elif block_type == "to_do":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            checked = "x" if block_data.get("checked") else " "
            return f"- [{checked}] {text}"

        elif block_type == "code":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            lang = block_data.get("language", "")
            return f"```{lang}\n{text}\n```"

        elif block_type == "quote":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            return f"> {text}"

        elif block_type == "divider":
            return "---"

        elif block_type == "callout":
            text = self._rich_text_to_string(block_data.get("rich_text", []))
            icon = block_data.get("icon", {}).get("emoji", "💡")
            return f"> {icon} {text}"

        elif block_type == "image":
            url = block_data.get("file", {}).get("url") or block_data.get("external", {}).get(
                "url", ""
            )
            caption = self._rich_text_to_string(block_data.get("caption", []))
            return f"![{caption}]({url})"

        elif block_type == "bookmark":
            url = block_data.get("url", "")
            return f"[Bookmark]({url})"

        elif block_type == "table":
            # Tables are complex, just note them
            return "[Table]"

        return ""

    def _rich_text_to_string(self, rich_text: list) -> str:
        """Convert Notion rich text array to plain string."""
        parts = []
        for item in rich_text:
            text = item.get("plain_text", "")
            annotations = item.get("annotations", {})

            # Apply formatting
            if annotations.get("bold"):
                text = f"**{text}**"
            if annotations.get("italic"):
                text = f"*{text}*"
            if annotations.get("code"):
                text = f"`{text}`"
            if annotations.get("strikethrough"):
                text = f"~~{text}~~"

            # Handle links
            if item.get("href"):
                text = f"[{text}]({item['href']})"

            parts.append(text)

        return "".join(parts)

    def _get_title(self, obj: dict) -> str:
        """Extract title from Notion object."""
        props = obj.get("properties", {})

        # Try common title properties
        for key in ("title", "Title", "Name", "name"):
            if key in props:
                title_prop = props[key]
                if title_prop.get("type") == "title":
                    title_items = title_prop.get("title", [])
                    if title_items:
                        return self._rich_text_to_string(title_items)

        # Fallback
        return "Untitled"

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all pages from Notion for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if credentials and credentials.get("access_token"):
            self._client = NotionClient(auth=credentials["access_token"])

        logger.info("Starting Notion fetch_source", uri=uri)

        # 1. List all pages
        try:
            pages = await self.list_pages(database_id=uri)
        except Exception as e:
            logger.error("Failed to list pages in Notion fetch_source", error=str(e))
            raise

        pages_to_fetch = []
        for page in pages:
            title = page["title"]
            path = f"{title}.md"

            def match_pattern(path_str: str, pattern: str) -> bool:
                if pattern == "**/*" or pattern == "*":
                    return True
                pat = pattern.replace("**/*", "*").replace("**", "*")
                return fnmatch.fnmatch(path_str, pat)

            included = any(match_pattern(path, p) for p in include_patterns)
            if not included:
                continue

            excluded = any(match_pattern(path, p) for p in exclude_patterns)
            if excluded:
                continue

            pages_to_fetch.append(page)

        logger.info(f"Found {len(pages_to_fetch)} pages to fetch from Notion")

        # 2. Fetch page content
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _fetch_page_concurrently(page):
            async with semaphore:
                try:
                    content_data = await self.get_page_content(page["id"])
                    content_data["path"] = content_data["filename"]
                    return content_data
                except Exception as e:
                    logger.warning(
                        "Failed to fetch page content during fetch_source",
                        page_id=page["id"],
                        error=str(e),
                    )
                    return None

        tasks = [_fetch_page_concurrently(page) for page in pages_to_fetch]
        results = await asyncio.gather(*tasks)

        downloaded_files = []
        total_size: int = 0
        for res in results:
            if res is not None:
                downloaded_files.append(res)
                total_size += int(res.get("size", 0))

        logger.info(
            "Notion fetch_source completed", pages=len(downloaded_files), total_size=total_size
        )
        return downloaded_files, total_size


# =============================================================================
# API Routes for Notion
# =============================================================================

notion_router = APIRouter(prefix="/api/v1/notion", tags=["Notion"])


class ConnectNotionRequest(BaseModel):
    workspace_name: str
    access_token: str


@notion_router.get("/pages")
async def list_pages(request: Request):
    """List available Notion pages."""
    logger.info("Listing Notion pages")
    return {"success": True, "pages": []}


@notion_router.post("")
async def connect_workspace(payload: ConnectNotionRequest, request: Request):
    """Connect a Notion workspace."""
    logger.info("Connecting Notion workspace", workspace_name=payload.workspace_name)
    workspace_id = str(uuid.uuid4())
    return {
        "success": True,
        "workspace_id": workspace_id,
        "message": "Notion workspace connected successfully",
    }


@notion_router.post("/{workspace_id}/sync")
async def sync_workspace(workspace_id: str, request: Request):
    """Sync Notion pages."""
    logger.info("Syncing Notion workspace", workspace_id=workspace_id)
    return {"success": True, "message": "Notion sync triggered"}


# Create a separate router for the callback to bypass /api/v1/notion prefix
notion_callback_router = APIRouter(tags=["Notion Callback"])


@notion_callback_router.get("/auth/notion/callback")
async def notion_oauth_callback(code: str, state: Optional[str] = None):
    """Handle OAuth callback from Notion."""
    try:
        from app.config import get_settings

        settings = get_settings()
        connector = NotionConnector(settings)
        token_data = await connector.exchange_code_for_token(code)

        return {
            "success": True,
            "access_token": token_data["access_token"],
            "workspace_name": token_data.get("workspace_name"),
            "workspace_id": token_data.get("workspace_id"),
            "bot_id": token_data.get("bot_id"),
            "message": "Notion connected successfully. You can return to the application.",
        }
    except Exception as e:
        logger.error("Notion OAuth callback error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
