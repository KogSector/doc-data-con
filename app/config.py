"""
Data Connector Service - Configuration
"""

from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field, validator
import tempfile
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Service configuration
    service_name: str = "data-connector"
    port: int = Field(alias="DATA_CONNECTOR_PORT")
    host: str = Field(alias="HOST")
    debug: bool = Field(alias="DEBUG")

    # PostgreSQL configuration (for sources and jobs management)
    database_url: str = Field(
        alias="POSTGRES_URL", description="PostgreSQL connection URL for sources and jobs"
    )

    # Downstream service URLs
    unified_processor_url: str = Field(alias="UNIFIED_PROCESSOR_URL")
    unified_processor_timeout_secs: int = Field(alias="UNIFIED_PROCESSOR_TIMEOUT_SECS", default=180)
    unified_processor_retry_attempts: int = Field(alias="UNIFIED_PROCESSOR_RETRY_ATTEMPTS", default=3)

    # Auth Service
    auth_service_url: str = Field(alias="AUTH_SERVICE_URL")
    # Auth middleware is contacted over HTTP; gRPC support has been removed from data-connector
    internal_api_key: str = Field(alias="INTERNAL_API_KEY")

    # CORS
    cors_origins: str = Field(alias="CORS_ORIGINS")

    # Base URL for OAuth Callbacks
    base_url: str = Field(alias="BASE_URL")

    # Google Drive Connector
    google_client_id: str | None = Field(default=None, alias="GOOGLE_CLIENT_ID")
    google_client_secret: str | None = Field(default=None, alias="GOOGLE_CLIENT_SECRET")
    google_redirect_uri: str | None = Field(default=None, alias="GOOGLE_REDIRECT_URI")

    # Notion Connector
    notion_api_key: str | None = Field(default=None, alias="NOTION_API_KEY")
    notion_client_id: str | None = Field(default=None, alias="NOTION_CLIENT_ID")
    notion_client_secret: str | None = Field(default=None, alias="NOTION_CLIENT_SECRET")
    notion_redirect_uri: str | None = Field(default=None, alias="NOTION_REDIRECT_URI")

    # GitHub OAuth
    github_client_id: str | None = Field(default=None, alias="GITHUB_CLIENT_ID")
    github_client_secret: str | None = Field(default=None, alias="GITHUB_CLIENT_SECRET")
    github_access_token: str | None = Field(default=None, alias="GITHUB_ACCESS_TOKEN")
    github_webhook_secret: str | None = Field(default=None, alias="GITHUB_WEBHOOK_SECRET")

    # GitLab OAuth
    gitlab_client_id: str | None = Field(default=None, alias="GITLAB_CLIENT_ID")
    gitlab_client_secret: str | None = Field(default=None, alias="GITLAB_CLIENT_SECRET")
    gitlab_webhook_secret: str | None = Field(default=None, alias="GITLAB_WEBHOOK_SECRET")

    # Bitbucket OAuth
    bitbucket_client_id: str | None = Field(default=None, alias="BITBUCKET_CLIENT_ID")
    bitbucket_client_secret: str | None = Field(default=None, alias="BITBUCKET_CLIENT_SECRET")

    # Microsoft OAuth (OneDrive/SharePoint)
    microsoft_client_id: str | None = Field(default=None, alias="MICROSOFT_CLIENT_ID")
    microsoft_client_secret: str | None = Field(default=None, alias="MICROSOFT_CLIENT_SECRET")
    microsoft_tenant_id: str | None = Field(default=None, alias="MICROSOFT_TENANT_ID")

    # Dropbox OAuth
    dropbox_client_id: str | None = Field(default=None, alias="DROPBOX_CLIENT_ID")
    dropbox_client_secret: str | None = Field(default=None, alias="DROPBOX_CLIENT_SECRET")

    # Security
    encryption_key: str = Field(
        alias="ENCRYPTION_KEY", description="32-byte encryption key for sensitive data"
    )

    jwt_secret_key: str = Field(
        alias="JWT_SECRET_KEY",
        description="Secret key for JWT credential reference signing (min 32 characters recommended)",
    )

    @validator("encryption_key")
    def validate_encryption_key(cls, v):
        if not v:
            raise ValueError("ENCRYPTION_KEY environment variable is required")
        if len(v) != 32:
            raise ValueError("ENCRYPTION_KEY must be exactly 32 characters long")
        return v

    @validator("jwt_secret_key")
    def validate_jwt_secret_key(cls, v):
        if not v:
            raise ValueError("JWT_SECRET_KEY environment variable is required")
        if len(v) < 32:
            raise ValueError("JWT_SECRET_KEY should be at least 32 characters long for security")
        return v

    @validator("database_url")
    def validate_database_url(cls, v):
        if not v:
            raise ValueError("DATABASE_URL environment variable is required")
        return v

    # Downloads folder configuration
    downloads_folder: str = Field(default_factory=lambda: os.path.join(tempfile.gettempdir(), "confuse-downloads"), alias="DOWNLOADS_BASE_PATH")



    environment: str = Field(alias="ENVIRONMENT")

    # FalkorDB Configuration
    falkordb_host: str = Field(
        alias="FALKORDB_HOST",
        default="r-6jissuruar.instance-tju0dagr0.hc-7up0crkyn.ap-south-1.aws.f2e0a955bb84.cloud",
    )
    falkordb_port: int = Field(alias="FALKORDB_PORT", default=64172)
    falkordb_username: str | None = Field(alias="FALKORDB_USERNAME", default="falkordb")
    falkordb_password: str | None = Field(alias="FALKORDB_PASSWORD", default="falkordb")

    class Config:
        env_file = [".env.map", ".env.secret", ".env.local"]
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
