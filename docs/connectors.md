# Data Source Connectors

> **Comprehensive External System Integration Guide**

## Overview

The Data Connector supports a wide range of external systems through standardized connectors. Each connector handles authentication, data fetching, and content formatting into a unified structure for processing by the unified-processor.

## Connector Architecture

All connectors implement a common interface for consistent behavior:

```python
class BaseConnector:
    async def authenticate(self) -> bool: ...
    async def list_files(self, path: str) -> List[FileMeta]: ...
    async def get_file_content(self, file_id: str) -> bytes: ...
    async def get_changes(self, since: datetime) -> List[Change]: ...
    async def get_metadata(self, file_id: str) -> FileMetadata: ...
```

## Code Repository Connectors

### GitHub Connector
**Authentication**: OAuth 2.0 App or Personal Access Token (PAT)  
**Sync Type**: Webhook + Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Full Repository Sync**: Complete repository tree traversal
- **Incremental Commits**: Delta processing for efficiency
- **Pull Request Analysis**: PR comments and discussions
- **Branch Support**: Multi-branch repository access
- **Release Management**: Tag and release asset handling
- **Issue Tracking**: Issue and comment synchronization

#### API Usage
- **GraphQL API**: Efficient tree traversal and bulk operations
- **REST API**: Fallback for specific operations
- **Rate Limiting**: Respect GitHub's 5,000 requests/hour limit
- **Webhook Support**: Real-time push/PR/Merge event processing

#### Configuration Example
```json
{
  "type": "github",
  "name": "Main Repository",
  "config": {
    "repository": "owner/repo-name",
    "branch": "main",
    "include_paths": ["src/", "docs/"],
    "exclude_paths": ["node_modules/", ".git/"],
    "sync_type": "webhook",
    "webhook_events": ["push", "pull_request", "issues"]
  }
}
```

### GitLab Connector
**Authentication**: OAuth 2.0 or Personal Access Token  
**Sync Type**: Webhook + Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Repository Tree**: Complete file structure access
- **Merge Requests**: MR content and discussion threads
- **CI/CD Pipelines**: Pipeline configuration and artifacts
- **Snippets**: Code snippet management
- **Wiki Pages**: Documentation synchronization
- **Multi-Project**: Group and project-level access

#### Configuration Example
```json
{
  "type": "gitlab",
  "name": "GitLab Project",
  "config": {
    "project_id": 123,
    "branch": "main",
    "include_merge_requests": true,
    "include_pipelines": false,
    "sync_type": "webhook"
  }
}
```

### Bitbucket Connector
**Authentication**: OAuth 2.0 or App Passwords  
**Sync Type**: Webhook + Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Source Code**: Repository content and history
- **Pull Requests**: PR content and review comments
- **Pipelines**: Build and deployment pipeline status
- **Wiki**: Project documentation
- **Issues**: Issue tracking and comments

## Document Storage Connectors

### Google Drive Connector
**Authentication**: OAuth 2.0 with Google Workspace  
**Sync Type**: Polling (Real-time via Drive API)  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **File Types**: Docs, Sheets, Slides, PDF, images, videos
- **Native Format Export**: Automatic conversion to standard formats
- **Shared Drives**: Team drive and shared folder support
- **Version History**: Access to document version history
- **Metadata**: Rich metadata extraction (owner, created, modified)
- **Permissions**: Respect file sharing permissions

#### Export Formats
- **Google Docs** → PDF, Markdown, HTML
- **Google Sheets** → CSV, XLSX, PDF
- **Google Slides** → PDF, PPTX, images
- **Other Files** → Original format preservation

#### Configuration Example
```json
{
  "type": "google_drive",
  "name": "Company Documents",
  "config": {
    "folder_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    "include_shared": true,
    "file_types": ["docs", "sheets", "pdf"],
    "export_format": "pdf",
    "sync_interval": "1h"
  }
}
```

### OneDrive Connector
**Authentication**: Microsoft Graph OAuth 2.0  
**Sync Type**: Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Personal & Business**: Both personal and business OneDrive
- **Office Documents**: Word, Excel, PowerPoint files
- **File Types**: PDF, images, text files
- **Sharing**: Shared folder and file access
- **Metadata**: Rich file metadata and versioning
- **Delta Sync**: Efficient change detection

#### Configuration Example
```json
{
  "type": "onedrive",
  "name": "OneDrive Documents",
  "config": {
    "drive_type": "business",
    "folder_path": "/Documents/Projects",
    "include_subfolders": true,
    "file_types": ["docx", "xlsx", "pdf"],
    "sync_interval": "30m"
  }
}
```

### Dropbox Connector
**Authentication**: OAuth 2.0  
**Sync Type**: Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Team Folders**: Business team folder support
- **File Types**: All file types supported
- **Version History**: Access to file versions
- **Shared Links**: Shared file and folder access
- **Large Files**: Streaming support for large files
- **Delta Sync**: Efficient change detection

#### Configuration Example
```json
{
  "type": "dropbox",
  "name": "Dropbox Storage",
  "config": {
    "folder_path": "/Projects",
    "include_team_folders": true,
    "max_file_size_mb": 100,
    "sync_interval": "15m"
  }
}
```

### Notion Connector
**Authentication**: OAuth 2.0 (Integration)  
**Sync Type**: Polling  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Page Blocks**: Recursive block tree traversal
- **Database Content**: Notion database records and properties
- **Markdown Conversion**: Semantic structure preservation
- **Rich Content**: Images, tables, code blocks
- **Relationships**: Page and database relationships
- **Comments**: Page and block comments

#### Content Processing
- **Block Tree Walking**: Recursive traversal of page blocks
- **Markdown Generation**: Semantic structure preservation for LLM
- **Database Export**: Structured data extraction
- **Media Handling**: Image and file attachment processing

#### Configuration Example
```json
{
  "type": "notion",
  "name": "Notion Workspace",
  "config": {
    "workspace_id": "workspace_id",
    "include_databases": true,
    "include_comments": true,
    "export_format": "markdown",
    "sync_interval": "1h"
  }
}
```

## Infrastructure Connectors

### S3 / MinIO Connector
**Authentication**: Access Key + Secret  
**Sync Type**: Polling (Event Notifications supported)  
**Status**: ✅ Fully Implemented  

#### Capabilities
- **Object Storage**: File and object ingestion
- **Event Notifications**: S3 event processing
- **Large Files**: Multipart upload/download support
- **Metadata**: S3 object metadata extraction
- **Versioning**: Object version history access
- **Lifecycle**: Object lifecycle management

#### Configuration Example
```json
{
  "type": "s3",
  "name": "S3 Storage",
  "config": {
    "bucket": "my-bucket",
    "region": "us-west-2",
    "prefix": "documents/",
    "event_notifications": true,
    "include_versions": false
  }
}
```

### Cloud Database Connector
**Authentication**: Connection String  
**Sync Type**: Polling (CDC optional)  
**Status**: ✅ Fully Implemented  

#### Supported Providers
- **AWS RDS**: PostgreSQL, MySQL, SQL Server
- **Azure SQL**: PostgreSQL, MySQL, SQL Server  
- **GCP Cloud SQL**: PostgreSQL, MySQL

#### Capabilities
- **Schema Discovery**: Automatic schema detection
- **Change Data Capture**: Real-time change tracking
- **Query Optimization**: Efficient bulk data extraction
- **Data Types**: Support for all common SQL data types
- **Security**: SSL/TLS connection support

#### Configuration Example
```json
{
  "type": "cloud_database",
  "name": "Production Database",
  "config": {
    "provider": "aws_rds",
    "engine": "postgresql",
    "host": "db.example.com",
    "port": 5432,
    "database": "app_db",
    "tables": ["users", "orders", "products"],
    "cdc_enabled": true,
    "sync_interval": "5m"
  }
}
```

## Implementation Details

### Authentication Strategies

#### OAuth 2.0 Flow
```python
class OAuth2Connector(BaseConnector):
    async def authenticate(self) -> bool:
        # 1. Redirect user to OAuth provider
        auth_url = self.get_authorization_url()
        
        # 2. Handle callback with authorization code
        code = await self.get_authorization_code()
        
        # 3. Exchange code for access token
        tokens = await self.exchange_code_for_tokens(code)
        
        # 4. Store tokens securely
        await self.store_tokens(tokens)
        
        return True
```

#### API Key Authentication
```python
class APIKeyConnector(BaseConnector):
    async def authenticate(self) -> bool:
        # Validate API key with provider
        response = await self.validate_api_key(self.api_key)
        return response.valid
```

### Data Fetching Strategies

#### Pagination Handling
```python
async def list_files(self, path: str) -> List[FileMeta]:
    all_files = []
    page = 1
    
    while True:
        files = await self.get_page(path, page)
        if not files:
            break
        all_files.extend(files)
        page += 1
    
    return all_files
```

#### Rate Limiting
```python
class RateLimitedConnector(BaseConnector):
    def __init__(self):
        self.rate_limiter = RateLimiter(
            max_requests=1000,
            time_window=3600  # 1 hour
        )
    
    async def make_request(self, endpoint: str):
        await self.rate_limiter.acquire()
        return await self.http_client.get(endpoint)
```

### Error Handling

#### Retry Logic
```python
async def get_file_content(self, file_id: str) -> bytes:
    for attempt in range(3):
        try:
            return await self._download_file(file_id)
        except (NetworkError, RateLimitError) as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
```

#### Error Classification
```python
class ConnectorError(Exception):
    def __init__(self, message: str, error_type: str, retry: bool = False):
        self.message = message
        self.error_type = error_type  # auth, network, rate_limit, etc.
        self.retry = retry
```

### Content Processing

#### File Type Detection
```python
def classify_file(self, filename: str, content: bytes) -> FileType:
    # Extension-based classification
    ext = Path(filename).suffix.lower()
    
    # Content-based classification for ambiguous files
    if ext in ['.txt', '.md']:
        if self.looks_like_code(content):
            return FileType.CODE
        return FileType.DOCUMENT
    
    return self.get_file_type_by_extension(ext)
```

#### Metadata Extraction
```python
async def get_metadata(self, file_id: str) -> FileMetadata:
    file_info = await self.get_file_info(file_id)
    
    return FileMetadata(
        file_id=file_id,
        filename=file_info.name,
        size=file_info.size,
        created_at=file_info.created_at,
        modified_at=file_info.modified_at,
        author=file_info.owner,
        permissions=file_info.permissions,
        custom_metadata=file_info.custom_fields
    )
```

## Best Practices

### Performance Optimization
- **Batch Operations**: Use bulk APIs when available
- **Parallel Processing**: Concurrent file downloads
- **Caching**: Cache metadata and frequently accessed content
- **Delta Sync**: Implement efficient change detection

### Security Considerations
- **Token Storage**: Encrypt stored access tokens
- **Scope Limitation**: Request minimal required permissions
- **Audit Logging**: Log all external API calls
- **Input Validation**: Validate all external data

### Error Recovery
- **Exponential Backoff**: Implement proper retry logic
- **Circuit Breaker**: Fail fast for persistent issues
- **Graceful Degradation**: Continue processing when possible
- **Error Reporting**: Detailed error classification

## Troubleshooting

### Common Issues

#### Authentication Failures
- Verify client credentials are correct
- Check redirect URI configuration
- Ensure proper OAuth scopes requested
- Validate token storage and refresh logic

#### Rate Limiting
- Monitor API usage against limits
- Implement proper backoff strategies
- Use efficient API calls (GraphQL, bulk operations)
- Consider premium API tiers if needed

#### Large File Handling
- Implement streaming for large downloads
- Monitor disk space usage
- Use appropriate timeout values
- Consider file size limits

#### Network Connectivity
- Implement proper timeout handling
- Use connection pooling
- Monitor network latency
- Implement retry logic for transient failures

## Development Guide

### Adding New Connectors

1. **Create Connector Class**
```python
class NewConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self.client = NewAPIClient(config.api_key)
```

2. **Implement Required Methods**
```python
async def authenticate(self) -> bool:
    # Implement authentication logic
    pass

async def list_files(self, path: str) -> List[FileMeta]:
    # Implement file listing
    pass

async def get_file_content(self, file_id: str) -> bytes:
    # Implement file download
    pass
```

3. **Add Configuration Schema**
```python
class NewConnectorConfig(BaseModel):
    api_key: str
    base_url: str
    timeout: int = 30
```

4. **Register Connector**
```python
CONNECTOR_REGISTRY.register("new_service", NewConnector)
```

### Testing Connectors

#### Unit Tests
```python
@pytest.mark.asyncio
async def test_connector_authentication():
    connector = NewConnector(test_config)
    result = await connector.authenticate()
    assert result is True
```

#### Integration Tests
```python
@pytest.mark.integration
async def test_real_connector():
    connector = NewConnector(real_config)
    files = await connector.list_files("/")
    assert len(files) > 0
```

#### Mock Testing
```python
@pytest.mark.asyncio
async def test_connector_with_mocks():
    with patch('new_module.NewAPIClient') as mock_client:
        connector = NewConnector(test_config)
        # Test with mocked responses
```

This comprehensive connector system provides robust, scalable integration with a wide variety of external data sources for the ConFuse platform.
