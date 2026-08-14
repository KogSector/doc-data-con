"""
Data Connector Service - Configuration
"""

from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field, validator, AliasChoices
import tempfile
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Service configuration
    service_name: str = "data-connector"
    port: int = Field(alias="DOC_DATA_CON_PORT")
    host: str = Field(alias="HOST")
    debug: bool = Field(alias="DEBUG")

    # PostgreSQL configuration (for sources and jobs management)
    database_url: str = Field(
        alias="DATABASE_URL", description="PostgreSQL connection URL for sources and jobs"
    )

    # Downstream service URLs
    doc_uni_proc_url: str = Field(validation_alias=AliasChoices("DOC_UNI_PROC_URL", "UNIFIED_PROCESSOR_URL"))
    doc_uni_proc_timeout_secs: int = Field(validation_alias=AliasChoices("DOC_UNI_PROC_TIMEOUT_SECS", "UNIFIED_PROCESSOR_TIMEOUT_SECS"), default=180)
    doc_uni_proc_retry_attempts: int = Field(validation_alias=AliasChoices("DOC_UNI_PROC_RETRY_ATTEMPTS", "UNIFIED_PROCESSOR_RETRY_ATTEMPTS"), default=15)

    # Auth Service
    auth_service_url: str = Field(alias="AUTH_SERVICE_URL")
    # Auth middleware is contacted over HTTP; gRPC support has been removed from data-connector
    internal_api_key: str = Field(alias="INTERNAL_API_KEY")

    # CORS
    cors_origins: str = Field(alias="CORS_ORIGINS")



    # Google Drive Connector
    google_client_id: str | None = Field(default=None, alias="GOOGLE_CLIENT_ID")
    google_client_secret: str | None = Field(default=None, alias="GOOGLE_CLIENT_SECRET")
    google_redirect_uri: str | None = Field(default=None, alias="GOOGLE_REDIRECT_URI")

    # Notion Connector
    notion_api_key: str | None = Field(default=None, alias="NOTION_API_KEY")
    notion_client_id: str | None = Field(default=None, alias="NOTION_CLIENT_ID")
    notion_client_secret: str | None = Field(default=None, alias="NOTION_CLIENT_SECRET")
    notion_redirect_uri: str | None = Field(default=None, alias="NOTION_REDIRECT_URI")


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

    # FalkorDB Configuration
    falkordb_host: str = Field(
        alias="FALKORDB_HOST",
        default="r-6jissuruar.instance-ivah2xvml.hc-7up0crkyn.ap-south-1.aws.f2e0a955bb84.cloud",
    )
    falkordb_port: int = Field(alias="FALKORDB_PORT", default=50860)
    falkordb_username: str | None = Field(alias="FALKORDB_USERNAME", default="adminconfuse")
    falkordb_password: str | None = Field(default="graph4confuse", alias="FALKORDB_PASSWORD")

    class Config:
        env_file = [".env.map", ".env.secret", ".env.local"]
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
