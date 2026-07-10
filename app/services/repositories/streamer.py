import structlog
import urllib.parse
from datetime import datetime, timezone
from app.config import get_settings
from app.connectors.github_client import GitHubConnector
from app.connectors.gitlab_client import GitLabConnector
from app.connectors.bitbucket_client import BitbucketConnector
from app.services.client import get_service_client
from app.infra.db.postgres import get_session, Repository


logger = structlog.get_logger()


class RepoStreamer:
    """
    Service for streaming repository files directly from Git providers and forwarding
    them to the unified-processor for chunking and embedding, without cloning to local disk.
    """

    def __init__(self):
        self.settings = get_settings()
        self.github = GitHubConnector(self.settings)
        self.gitlab = GitLabConnector(self.settings)
        self.bitbucket = BitbucketConnector(self.settings)
        self.client = get_service_client()

    def _parse_repo_url(self, url: str, provider: str) -> tuple[str, str]:
        import re

        folder_path = ""
        repo_id = ""
        url = url.rstrip("/")

        if provider == "github":
            match = re.search(r"github\.com/([^/]+/[^/]+?)(?:/tree/[^/]+/(.+))?$", url)
            if match:
                repo_id = match.group(1)
                if repo_id.endswith(".git"):
                    repo_id = repo_id[:-4]
                if match.group(2):
                    folder_path = match.group(2).rstrip("/")
            else:
                parts = url.split("/")
                repo_id = (
                    f"{parts[-2]}/{parts[-1][:-4] if parts[-1].endswith('.git') else parts[-1]}"
                )
        elif provider == "gitlab":
            match = re.search(r"gitlab\.com/([^/]+/[^/]+?)(?:/-/tree/[^/]+/(.+?))?(?:\?.*)?$", url)
            if match:
                repo_id = match.group(1)
                if repo_id.endswith(".git"):
                    repo_id = repo_id[:-4]
                if match.group(2):
                    folder_path = match.group(2).rstrip("/")
            else:
                parts = url.split("/")
                repo_id = (
                    f"{parts[-2]}/{parts[-1][:-4] if parts[-1].endswith('.git') else parts[-1]}"
                )
        elif provider == "bitbucket":
            match = re.search(r"bitbucket\.org/([^/]+/[^/]+?)(?:/src/[^/]+/(.+?))?(?:\?.*)?$", url)
            if match:
                repo_id = match.group(1)
                if repo_id.endswith(".git"):
                    repo_id = repo_id[:-4]
                if match.group(2):
                    folder_path = match.group(2).rstrip("/")
            else:
                parts = url.split("/")
                repo_id = (
                    f"{parts[-2]}/{parts[-1][:-4] if parts[-1].endswith('.git') else parts[-1]}"
                )

        return repo_id, folder_path

    async def stream_repository(
        self,
        repo_id: str,
        provider: str,
        url: str,
        branch: str,
        access_token: str,
        user_id: str = "system",
    ):
        """
        Stream repository files and send them to unified-processor.
        """
        logger.info(
            "[REPO-STREAMER] Starting repository stream",
            repo_id=repo_id,
            provider=provider,
            url=url,
            user_id=user_id,
        )

        try:
            repo_path, folder_path = self._parse_repo_url(url, provider)

            last_sync = None
            # Update repository status to sync_in_progress
            async with get_session() as session:
                repo = await session.get(Repository, repo_id)
                if repo:
                    last_sync = repo.last_sync
                    repo.status = "sync_in_progress"
                    repo.last_sync = datetime.now(timezone.utc)

                    # Fetch repository metadata
                    try:
                        if provider == "github":
                            repo_name = repo_path
                            if access_token:
                                self.github.set_credentials(access_token)
                            else:
                                from github import Github as GH

                                self.github._github = GH()
                            import asyncio

                            def _get_gh_meta():
                                r = self.github.github.get_repo(repo_name)
                                return (
                                    r.description,
                                    r.language,
                                    getattr(r, "stargazers_count", 0),
                                    getattr(r, "forks_count", 0),
                                )

                            desc, lang, stars, forks = await asyncio.to_thread(_get_gh_meta)
                            repo.description = desc
                            repo.language = lang
                            repo.stars = stars
                            repo.forks = forks
                        elif provider == "gitlab":
                            project_id = urllib.parse.quote(repo_path, safe="")
                            self.gitlab.set_credentials(access_token)
                            import asyncio

                            def _get_gl_meta():
                                p = self.gitlab.gl.projects.get(project_id)
                                return (
                                    p.description,
                                    getattr(p, "star_count", 0),
                                    getattr(p, "forks_count", 0),
                                )

                            desc, stars, forks = await asyncio.to_thread(_get_gl_meta)
                            repo.description = desc
                            repo.stars = stars
                            repo.forks = forks
                    except Exception as meta_err:
                        logger.warning(
                            "Failed to fetch repo metadata during stream",
                            repo_id=repo_id,
                            error=str(meta_err),
                        )

                    await session.commit()

            # Extract owner and repo from URL
            if provider == "github":
                repo_full_name = repo_path

                if access_token:
                    self.github.set_credentials(access_token)
                else:
                    from github import Github as GH

                    self.github._github = GH()

                files_to_sync = []
                if last_sync:
                    logger.info(
                        "[REPO-STREAMER] Performing incremental sync since", last_sync=last_sync
                    )
                    events = await self.github.poll_changes(last_sync, repo_full_name, branch)

                    file_status = {}
                    for ev in events:
                        file_status[ev["resource_path"]] = ev["event_type"]

                    for fpath, status in file_status.items():
                        if folder_path and not (
                            fpath.startswith(folder_path + "/") or fpath == folder_path
                        ):
                            continue
                        files_to_sync.append({"path": fpath, "status": status})
                else:
                    logger.info("[REPO-STREAMER] Performing full sync")
                    tree = await self.github.get_repository_tree(
                        repo_full_name, branch=branch, recursive=True
                    )
                    # Filter blob files
                    files = [item for item in tree if item["type"] == "blob"]
                    if folder_path:
                        files = [
                            item
                            for item in files
                            if item["path"].startswith(folder_path + "/")
                            or item["path"] == folder_path
                        ]
                    for f in files:
                        files_to_sync.append({"path": f["path"], "status": "added"})

                for file_item in files_to_sync:
                    file_path = file_item["path"]
                    # Skip binary files based on extensions
                    if self._is_binary_file(file_path):
                        continue

                    if file_item["status"] in ("deleted", "removed"):
                        logger.info("Streaming deleted file event", file_path=file_path)
                        await self._send_to_processor(
                            repo_id, file_path, "", url, user_id, is_deleted=True
                        )
                        continue

                    try:
                        file_data = await self.github.download_file(
                            repo_full_name, file_path, ref=branch
                        )
                        content = file_data["content"]
                        if isinstance(content, bytes):
                            content = content.decode("utf-8", errors="replace")

                        await self._send_to_processor(repo_id, file_path, content, url, user_id)
                    except Exception as e:
                        logger.warning("Failed to stream file", file_path=file_path, error=str(e))

            elif provider == "gitlab":
                project_id = urllib.parse.quote(repo_path, safe="")

                self.gitlab.set_credentials(access_token)
                tree = await self.gitlab.get_repository_tree(project_id, ref=branch, recursive=True)

                files = [item for item in tree if item["type"] == "blob"]
                if folder_path:
                    files = [
                        item
                        for item in files
                        if item["path"].startswith(folder_path + "/") or item["path"] == folder_path
                    ]

                for file_item in files:
                    file_path = file_item["path"]
                    if self._is_binary_file(file_path):
                        continue

                    try:
                        file_data = await self.gitlab.download_file(
                            project_id, file_path, ref=branch
                        )
                        content = file_data["content"]
                        if isinstance(content, bytes):
                            content = content.decode("utf-8", errors="replace")

                        await self._send_to_processor(repo_id, file_path, content, url, user_id)
                    except Exception as e:
                        logger.warning("Failed to stream file", file_path=file_path, error=str(e))

            elif provider == "bitbucket":
                workspace, repo_slug = repo_path.split("/", 1)

                self.bitbucket.set_credentials(access_token)
                tree = await self.bitbucket.get_repository_tree(workspace, repo_slug, commit=branch)

                files = [item for item in tree if item["type"] == "commit_file"]
                if folder_path:
                    files = [
                        item
                        for item in files
                        if item["path"].startswith(folder_path + "/") or item["path"] == folder_path
                    ]

                for file_item in files:
                    file_path = file_item["path"]
                    if self._is_binary_file(file_path):
                        continue

                    try:
                        file_data = await self.bitbucket.download_file(
                            workspace, repo_slug, file_path, branch
                        )
                        content = file_data["content"]
                        if isinstance(content, bytes):
                            content = content.decode("utf-8", errors="replace")

                        await self._send_to_processor(repo_id, file_path, content, url, user_id)
                    except Exception as e:
                        logger.warning("Failed to stream file", file_path=file_path, error=str(e))

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            # No Kafka producer to flush

            # Update repository status to active
            async with get_session() as session:
                repo = await session.get(Repository, repo_id)
                if repo:
                    repo.status = "active"
                    await session.commit()

            logger.info("[REPO-STREAMER] Successfully streamed repository, triggering cross-file graph sync", repo_id=repo_id)
            try:
                # Trigger cross-file relationship building in unified-processor
                payload = {
                    "source_id": repo_id
                }
                await self.client.send_to_processor_http(
                    endpoint="/api/v1/graph/sync", payload=payload, timeout=30.0, headers={"x-user-id": user_id}
                )
                logger.info("[REPO-STREAMER] Graph sync triggered successfully", repo_id=repo_id)
            except Exception as sync_err:
                logger.warning(
                    "[REPO-STREAMER] Failed to trigger graph sync",
                    repo_id=repo_id,
                    error=str(sync_err)
                )

        except Exception as e:
            logger.error(
                "[REPO-STREAMER] Failed to stream repository", repo_id=repo_id, error=str(e)
            )
            async with get_session() as session:
                repo = await session.get(Repository, repo_id)
                if repo:
                    repo.status = "sync_failed"
                    await session.commit()

    async def _send_to_processor(
        self,
        repo_id: str,
        file_path: str,
        content: str,
        repo_url: str,
        user_id: str = "system",
        is_deleted: bool = False,
    ):
        try:
            payload = {
                "content": content,
                "filename": file_path,
                "source_id": repo_id,
                "user_id": user_id,
                "is_base64": False
            }
            await self.client.send_to_processor_http(
                endpoint="/api/v1/process", payload=payload, timeout=60.0
            )
        except Exception as e:
            logger.error(
                "Error sending file to unified-processor via HTTP",
                file_path=file_path,
                error=str(e),
            )
            raise

    def _is_binary_file(self, file_path: str) -> bool:
        """Simple check to skip binary files based on extension."""
        binary_exts = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".bmp",
            ".ico",
            ".pdf",
            ".zip",
            ".tar",
            ".gz",
            ".7z",
            ".rar",
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".bin",
            ".whl",
            ".mp4",
            ".mp3",
            ".wav",
            ".ttf",
            ".woff",
            ".woff2",
            ".eot",
            ".svg",
            ".webp",
            ".tiff",
            ".otf",
            ".ogg",
        }
        for ext in binary_exts:
            if file_path.lower().endswith(ext):
                return True
        return False


_repo_streamer_instance = None


def get_repo_streamer() -> RepoStreamer:
    global _repo_streamer_instance
    if _repo_streamer_instance is None:
        _repo_streamer_instance = RepoStreamer()
    return _repo_streamer_instance
