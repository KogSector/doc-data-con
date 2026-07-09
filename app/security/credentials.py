from __future__ import annotations
from cryptography.hazmat.primitives import hashes
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
import os
from dataclasses import dataclass
import structlog
from cryptography.fernet import Fernet, InvalidToken
import base64
import json
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

"""
Unified Credentials Module

Handles JWT generation and secure PostgreSQL storage.
"""
logger = structlog.get_logger()

# JWT import - optional
JWT_AVAILABLE = False
try:
    import jwt

    JWT_AVAILABLE = True
except ImportError:
    logger.warning("PyJWT not available. Install: pip install PyJWT")


@dataclass
class CredentialClaims:
    """Claims included in credential reference JWT."""

    provider: str
    repo_id: str
    user_id: str
    organization_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JWT payload."""
        claims = {
            "provider": self.provider,
            "repo_id": self.repo_id,
            "user_id": self.user_id,
        }
        if self.organization_id:
            claims["organization_id"] = self.organization_id
        return claims


class CredentialJWTGenerator:
    """
    JWT token generator for credential references.

    Features:
    - Short-lived tokens (5-minute expiry)
    - Secure signing with environment-based secret
    - Claims: provider, repo_id, user_id
    - HS256 algorithm
    """

    # Token expiry: 5 minutes as per requirements
    TOKEN_EXPIRY_MINUTES = 5

    # JWT algorithm
    ALGORITHM = "HS256"

    def __init__(self, secret_key: Optional[str] = None):
        """
        Initialize JWT generator.

        Args:
            secret_key: Secret key for JWT signing. If None, reads from JWT_SECRET_KEY env var.

        Raises:
            ImportError: If PyJWT is not installed
            ValueError: If secret_key is not provided and JWT_SECRET_KEY env var is not set
        """
        if not JWT_AVAILABLE:
            raise ImportError("PyJWT not available. Install: pip install PyJWT")

        self._secret_key = secret_key or os.environ.get("JWT_SECRET_KEY")

        if not self._secret_key:
            raise ValueError(
                "JWT secret key not provided. Set JWT_SECRET_KEY environment variable "
                "or pass secret_key parameter."
            )

        if len(self._secret_key) < 32:
            logger.warning(
                "JWT secret key is shorter than recommended 32 characters",
                length=len(self._secret_key),
            )

        logger.info(
            "CredentialJWTGenerator initialized",
            algorithm=self.ALGORITHM,
            expiry_minutes=self.TOKEN_EXPIRY_MINUTES,
        )

    def generate_credential_ref(
        self,
        provider: str,
        repo_id: str,
        user_id: str,
        organization_id: Optional[str] = None,
    ) -> str:
        """
        Generate a credential reference JWT token.

        Args:
            provider: Provider name (github, gitlab, bitbucket, etc.)
            repo_id: Repository identifier (UUID)
            user_id: User identifier (UUID)
            organization_id: Optional organization identifier (UUID)

        Returns:
            JWT token string (credential_ref)

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        # Validate required parameters
        if not provider:
            raise ValueError("provider is required")
        if not repo_id:
            raise ValueError("repo_id is required")
        if not user_id:
            raise ValueError("user_id is required")

        # Create claims
        claims = CredentialClaims(
            provider=provider,
            repo_id=repo_id,
            user_id=user_id,
            organization_id=organization_id,
        )

        # Build JWT payload
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(minutes=self.TOKEN_EXPIRY_MINUTES)

        payload = {
            **claims.to_dict(),
            "iat": now,  # Issued at
            "exp": expiry,  # Expiry
            "nbf": now,  # Not before
        }

        # Generate JWT
        try:
            token = jwt.encode(payload, self._secret_key, algorithm=self.ALGORITHM)

            logger.info(
                "Generated credential_ref JWT",
                provider=provider,
                repo_id=repo_id,
                user_id=user_id,
                expires_at=expiry.isoformat(),
            )

            return token
        except Exception as e:
            logger.error(
                "Failed to generate credential_ref JWT",
                provider=provider,
                repo_id=repo_id,
                error=str(e),
            )
            raise

    def verify_credential_ref(self, token: str) -> CredentialClaims:
        """
        Verify and decode a credential reference JWT token.

        Args:
            token: JWT token string

        Returns:
            CredentialClaims extracted from token

        Raises:
            jwt.ExpiredSignatureError: If token is expired
            jwt.InvalidTokenError: If token is invalid
        """
        try:
            payload = jwt.decode(token, self._secret_key, algorithms=[self.ALGORITHM])

            claims = CredentialClaims(
                provider=payload["provider"],
                repo_id=payload["repo_id"],
                user_id=payload["user_id"],
                organization_id=payload.get("organization_id"),
            )

            logger.info(
                "Verified credential_ref JWT",
                provider=claims.provider,
                repo_id=claims.repo_id,
                user_id=claims.user_id,
            )

            return claims
        except jwt.ExpiredSignatureError:
            logger.warning("Credential_ref JWT expired", token_preview=token[:20])
            raise
        except jwt.InvalidTokenError as e:
            logger.error("Invalid credential_ref JWT", error=str(e))
            raise

    def get_token_expiry(self, token: str) -> Optional[datetime]:
        """
        Get expiry time from token without full verification.

        Args:
            token: JWT token string

        Returns:
            Expiry datetime or None if cannot be determined
        """
        try:
            # Decode without verification to get expiry
            payload = jwt.decode(token, options={"verify_signature": False})

            exp = payload.get("exp")
            if exp:
                return datetime.fromtimestamp(exp, tz=timezone.utc)

            return None
        except Exception as e:
            logger.error("Failed to get token expiry", error=str(e))
            return None

    def is_token_expired(self, token: str) -> bool:
        """
        Check if token is expired without full verification.

        Args:
            token: JWT token string

        Returns:
            True if expired, False otherwise
        """
        expiry = self.get_token_expiry(token)
        if not expiry:
            return True

        return datetime.now(timezone.utc) >= expiry


# Singleton instance for convenience
_generator_instance: Optional[CredentialJWTGenerator] = None


def get_jwt_generator(secret_key: Optional[str] = None) -> CredentialJWTGenerator:
    """
    Get or create singleton JWT generator instance.

    Args:
        secret_key: Optional secret key. If None, uses JWT_SECRET_KEY env var.

    Returns:
        CredentialJWTGenerator instance
    """
    global _generator_instance

    if _generator_instance is None:
        _generator_instance = CredentialJWTGenerator(secret_key=secret_key)

    return _generator_instance


logger = structlog.get_logger()


@dataclass
class CredentialData:
    """Credential data structure for storage."""

    repo_id: str
    provider: str
    user_id: str
    access_token: str
    refresh_token: Optional[str] = None
    organization_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        """Convert to dictionary for encryption."""
        return {
            "repo_id": self.repo_id,
            "provider": self.provider,
            "user_id": self.user_id,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "organization_id": self.organization_id,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> CredentialData:
        """Create from dictionary after decryption."""
        return cls(
            repo_id=data["repo_id"],
            provider=data["provider"],
            user_id=data["user_id"],
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            organization_id=data.get("organization_id"),
            expires_at=datetime.fromisoformat(data["expires_at"])
            if data.get("expires_at")
            else None,
            created_at=datetime.fromisoformat(data["created_at"])
            if data.get("created_at")
            else datetime.now(timezone.utc),
        )

    def is_expired(self) -> bool:
        """Check if credential is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) >= self.expires_at


class CredentialEncryption:
    """
    Handles encryption and decryption of credentials using Fernet (AES-256).
    """

    def __init__(self, encryption_key: str, salt: bytes = b"confuse_credential_salt"):
        """
        Initialize credential encryption.

        Args:
            encryption_key: Base encryption key (32 characters)
            salt: Salt for key derivation
        """
        if len(encryption_key) != 32:
            raise ValueError("Encryption key must be exactly 32 characters")

        self._fernet = self._derive_key(encryption_key, salt)
        logger.info("CredentialEncryption initialized with AES-256")

    def _derive_key(self, passphrase: str, salt: bytes) -> Fernet:
        """Derive encryption key from passphrase using PBKDF2."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode()))
        return Fernet(key)

    def encrypt(self, credential: CredentialData) -> str:
        """
        Encrypt credential data.

        Args:
            credential: Credential to encrypt

        Returns:
            Encrypted credential string (base64)
        """
        data = json.dumps(credential.to_dict())
        encrypted = self._fernet.encrypt(data.encode())
        return encrypted.decode()

    def decrypt(self, encrypted: str) -> CredentialData:
        """
        Decrypt credential data.

        Args:
            encrypted: Encrypted credential string

        Returns:
            Decrypted CredentialData

        Raises:
            ValueError: If decryption fails or data is invalid
        """
        try:
            decrypted = self._fernet.decrypt(encrypted.encode())
            data = json.loads(decrypted)
            return CredentialData.from_dict(data)
        except InvalidToken:
            raise ValueError("Invalid or corrupted credential data")
        except json.JSONDecodeError:
            raise ValueError("Invalid credential format")


class CredentialStorage:
    """
    Manages credential storage and retrieval in PostgreSQL.

    Credentials are encrypted at rest and associated with credential_ref JWTs.
    """

    def __init__(self, encryption: CredentialEncryption, db_session_factory):
        """
        Initialize credential storage.

        Args:
            encryption: CredentialEncryption instance
            db_session_factory: Database session factory (from postgres.py)
        """
        self._encryption = encryption
        self._session_factory = db_session_factory
        logger.info("CredentialStorage initialized")

    async def store_credential(
        self,
        repo_id: str,
        provider: str,
        user_id: str,
        access_token: str,
        refresh_token: Optional[str] = None,
        organization_id: Optional[str] = None,
        expires_in: Optional[int] = None,
    ) -> bool:
        """
        Store encrypted credential in PostgreSQL.

        Args:
            repo_id: Repository identifier (UUID)
            provider: Provider name (github, gitlab, bitbucket)
            user_id: User identifier (UUID)
            access_token: OAuth access token
            refresh_token: OAuth refresh token (optional)
            organization_id: Organization identifier (optional)
            expires_in: Token expiry in seconds (optional)

        Returns:
            True if stored successfully
        """
        from datetime import timedelta

        expires_at = None
        if expires_in:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        credential = CredentialData(
            repo_id=repo_id,
            provider=provider,
            user_id=user_id,
            access_token=access_token,
            refresh_token=refresh_token,
            organization_id=organization_id,
            expires_at=expires_at,
        )

        encrypted = self._encryption.encrypt(credential)

        try:
            from sqlalchemy import text

            async with self._session_factory() as session:
                # Upsert credential
                query = text("""
                    INSERT INTO credentials (repo_id, user_id, provider, encrypted_data, expires_at, created_at, updated_at)
                    VALUES (:repo_id, :user_id, :provider, :encrypted_data, :expires_at, :created_at, :updated_at)
                    ON CONFLICT (repo_id) 
                    DO UPDATE SET 
                        encrypted_data = EXCLUDED.encrypted_data,
                        expires_at = EXCLUDED.expires_at,
                        updated_at = EXCLUDED.updated_at
                """)

                await session.execute(
                    query,
                    {
                        "repo_id": repo_id,
                        "user_id": user_id,
                        "provider": provider,
                        "encrypted_data": encrypted,
                        "expires_at": expires_at,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                await session.commit()

            logger.info(
                "Stored encrypted credential",
                repo_id=repo_id,
                provider=provider,
                user_id=user_id,
            )
            return True

        except Exception as e:
            logger.error("Failed to store credential", repo_id=repo_id, error=str(e))
            return False

    async def get_credential(self, repo_id: str) -> Optional[CredentialData]:
        """
        Retrieve and decrypt credential by repo_id.

        Args:
            repo_id: Repository identifier (UUID)

        Returns:
            CredentialData or None if not found
        """
        try:
            from sqlalchemy import text

            async with self._session_factory() as session:
                query = text("""
                    SELECT encrypted_data, expires_at 
                    FROM credentials 
                    WHERE repo_id = :repo_id
                """)

                result = await session.execute(query, {"repo_id": repo_id})
                row = result.fetchone()

                if not row:
                    logger.warning("Credential not found", repo_id=repo_id)
                    return None

                encrypted_data = row[0]
                expires_at = row[1]

                # Decrypt credential
                credential = self._encryption.decrypt(encrypted_data)

                # Check expiry
                if credential.is_expired():
                    logger.warning(
                        "Credential expired",
                        repo_id=repo_id,
                        expires_at=credential.expires_at.isoformat()
                        if credential.expires_at
                        else None,
                    )
                    return None

                logger.info("Retrieved credential", repo_id=repo_id, provider=credential.provider)
                return credential

        except Exception as e:
            logger.error("Failed to get credential", repo_id=repo_id, error=str(e))
            return None

    async def delete_credential(self, repo_id: str) -> bool:
        """
        Delete credential by repo_id.

        Args:
            repo_id: Repository identifier (UUID)

        Returns:
            True if deleted successfully
        """
        try:
            from sqlalchemy import text

            async with self._session_factory() as session:
                query = text("DELETE FROM credentials WHERE repo_id = :repo_id")
                result = await session.execute(query, {"repo_id": repo_id})
                await session.commit()

                deleted = result.rowcount > 0

                if deleted:
                    logger.info("Deleted credential", repo_id=repo_id)
                else:
                    logger.warning("Credential not found for deletion", repo_id=repo_id)

                return deleted

        except Exception as e:
            logger.error("Failed to delete credential", repo_id=repo_id, error=str(e))
            return False


# Singleton instances
_encryption_instance: Optional[CredentialEncryption] = None
_storage_instance: Optional[CredentialStorage] = None


def init_credential_storage(encryption_key: str, db_session_factory) -> CredentialStorage:
    """
    Initialize credential storage singleton.

    Args:
        encryption_key: 32-character encryption key
        db_session_factory: Database session factory

    Returns:
        CredentialStorage instance
    """
    global _encryption_instance, _storage_instance

    if _encryption_instance is None:
        _encryption_instance = CredentialEncryption(encryption_key)

    if _storage_instance is None:
        _storage_instance = CredentialStorage(_encryption_instance, db_session_factory)

    return _storage_instance


def get_credential_storage() -> CredentialStorage:
    """
    Get credential storage singleton.

    Returns:
        CredentialStorage instance

    Raises:
        RuntimeError: If not initialized
    """
    if _storage_instance is None:
        raise RuntimeError(
            "CredentialStorage not initialized. Call init_credential_storage() first."
        )

    return _storage_instance
