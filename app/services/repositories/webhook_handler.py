"""
Universal Webhook Handler - Handle webhooks from all data source providers.

Provides signature verification, payload parsing, and conversion to
universal SyncEvent format.
"""

from __future__ import annotations

import hashlib
import hmac
import structlog


logger = structlog.get_logger()


class UniversalWebhookHandler:
    """
    Universal webhook endpoint with provider-specific converters.

    Responsibilities:
    - Verify webhook signatures
    - Parse provider-specific payloads
    - Convert to universal SyncEvent format
    - Handle webhook registration/deregistration
    """

    def __init__(self, secrets: dict[str, str] = None):
        """
        Initialize webhook handler.

        Args:
            secrets: Map of source_id -> webhook secret for verification
        """
        self._secrets: dict[str, str] = secrets or {}

    # =========================================================================
    # Signature Verification
    # =========================================================================

    def verify_github_signature(
        self,
        payload: bytes,
        signature: str,
        secret: str,
    ) -> bool:
        """Verify GitHub webhook signature (X-Hub-Signature-256)."""
        if not signature.startswith("sha256="):
            return False

        expected = (
            "sha256="
            + hmac.new(
                secret.encode(),
                payload,
                hashlib.sha256,
            ).hexdigest()
        )

        return hmac.compare_digest(expected, signature)

    def verify_gitlab_signature(
        self,
        token: str,
        secret: str,
    ) -> bool:
        """Verify GitLab webhook token (X-Gitlab-Token)."""
        return hmac.compare_digest(token, secret)

    def verify_bitbucket_signature(
        self,
        payload: bytes,
        signature: str,
        secret: str,
    ) -> bool:
        """Verify Bitbucket webhook signature."""
        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected, signature)

    def verify_signature(
        self,
        provider: str,
        payload: bytes,
        headers: dict[str, str],
        source_id: str,
    ) -> bool:
        """
        Verify webhook signature based on provider.

        Args:
            provider: Provider type (github, gitlab, bitbucket)
            payload: Raw request body
            headers: Request headers
            source_id: Source ID to look up secret

        Returns:
            True if signature is valid
        """
        secret = self._secrets.get(source_id)
        if not secret:
            logger.warning("No secret registered for source", source_id=source_id)
            return False

        if provider == "github":
            signature = headers.get("x-hub-signature-256", "")
            return self.verify_github_signature(payload, signature, secret)

        elif provider == "gitlab":
            token = headers.get("x-gitlab-token", "")
            return self.verify_gitlab_signature(token, secret)

        elif provider == "bitbucket":
            signature = headers.get("x-hub-signature", "")
            return self.verify_bitbucket_signature(payload, signature, secret)

        else:
            logger.warning("Unknown provider for signature verification", provider=provider)
            return True  # Skip verification for unknown providers

    # =========================================================================
    # Commit Information Extraction (Event-Driven Pipeline)
    # =========================================================================

    def extract_github_commits(self, payload: dict) -> tuple[str, str, str, str] | None:
        """
        Extract commit information from GitHub push webhook.

        Args:
            payload: GitHub webhook payload

        Returns:
            Tuple of (repo_url, branch, old_commit, new_commit) or None if invalid
        """
        try:
            repo = payload.get("repository", {})
            repo_url = repo.get("clone_url", "")

            ref = payload.get("ref", "")
            branch = ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ref

            # GitHub provides before and after commits
            old_commit = payload.get("before", "")
            new_commit = payload.get("after", "")

            # Validate commit SHAs (40-character hex strings)
            if not (len(old_commit) == 40 and len(new_commit) == 40):
                logger.warning(
                    "Invalid commit SHAs in GitHub webhook",
                    old_commit=old_commit,
                    new_commit=new_commit,
                )
                return None

            if not repo_url or not branch:
                logger.warning("Missing repo_url or branch in GitHub webhook")
                return None

            return (repo_url, branch, old_commit, new_commit)

        except Exception as e:
            logger.error("Failed to extract GitHub commits", error=str(e))
            return None

    def extract_gitlab_commits(self, payload: dict) -> tuple[str, str, str, str] | None:
        """
        Extract commit information from GitLab push webhook.

        Args:
            payload: GitLab webhook payload

        Returns:
            Tuple of (repo_url, branch, old_commit, new_commit) or None if invalid
        """
        try:
            project = payload.get("project", {})
            repo_url = project.get("git_http_url", "")

            ref = payload.get("ref", "")
            branch = ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ref

            # GitLab provides before and after commits
            old_commit = payload.get("before", "")
            new_commit = payload.get("after", "")

            # Validate commit SHAs (40-character hex strings)
            if not (len(old_commit) == 40 and len(new_commit) == 40):
                logger.warning(
                    "Invalid commit SHAs in GitLab webhook",
                    old_commit=old_commit,
                    new_commit=new_commit,
                )
                return None

            if not repo_url or not branch:
                logger.warning("Missing repo_url or branch in GitLab webhook")
                return None

            return (repo_url, branch, old_commit, new_commit)

        except Exception as e:
            logger.error("Failed to extract GitLab commits", error=str(e))
            return None

    def extract_bitbucket_commits(self, payload: dict) -> tuple[str, str, str, str] | None:
        """
        Extract commit information from Bitbucket push webhook.

        Args:
            payload: Bitbucket webhook payload

        Returns:
            Tuple of (repo_url, branch, old_commit, new_commit) or None if invalid
        """
        try:
            repo = payload.get("repository", {})
            links = repo.get("links", {})
            html_link = links.get("html", {})
            repo_url = html_link.get("href", "")

            # Bitbucket provides changes array
            changes = payload.get("push", {}).get("changes", [])
            if not changes:
                logger.warning("No changes in Bitbucket webhook")
                return None

            # Get first change (typically only one for push events)
            change = changes[0]
            old = change.get("old", {})
            new = change.get("new", {})

            branch = new.get("name", "")
            old_commit = old.get("target", {}).get("hash", "") if old else ""
            new_commit = new.get("target", {}).get("hash", "")

            # Bitbucket uses shorter commit hashes, but we need full 40-char
            # For now, accept any non-empty hash
            if not old_commit or not new_commit:
                logger.warning(
                    "Missing commit hashes in Bitbucket webhook",
                    old_commit=old_commit,
                    new_commit=new_commit,
                )
                return None

            if not repo_url or not branch:
                logger.warning("Missing repo_url or branch in Bitbucket webhook")
                return None

            return (repo_url, branch, old_commit, new_commit)

        except Exception as e:
            logger.error("Failed to extract Bitbucket commits", error=str(e))
            return None
