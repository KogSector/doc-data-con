# Data Connector Service

**Port**: 8080  
**Role**: Universal source integration and intelligent file routing

## Overview

The Data Connector service is the entry point for all data sources in the ConFuse platform. It handles:

- **Source Management**: Connect to Git repositories, document storage, APIs
- **File Type Detection**: Analyze files to determine if they're code or documents
- **Intelligent Routing**: Route files to appropriate processors via HTTP
- **Document Processing**: Send documents to unified-processor for chunking and embedding
- **Webhook Handling**: Receive triggers from external systems

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Connector (:8080)                    │
├─────────────────────────────────────────────────────────────┤
│  Source Management  │  File Classification  │  HTTP Client   │
└─────────────────────┴──────────────────────┴───────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Unified        │
                    │  Processor     │
                    └─────────────────┘
```

## Supported Sources

### Code Repositories
- GitHub
- GitLab
- Bitbucket

### Document Storage
- Google Drive
- OneDrive
- Dropbox
- Notion

### File Types
- **Code**: Python, JavaScript, TypeScript, Java, Go, Rust, C/C++
- **Documents**: PDF, Word, Markdown, Text
- **Configuration**: YAML, JSON, TOML

## Processing Flow

### Document Ingestion
```
Document Upload → File Discovery → Classification → HTTP Processing
```

### Processing Steps
```
1. Document Upload
2. File Classification
3. HTTP POST to unified-processor
4. Chunking and Embedding
```

## Configuration

### Environment Variables
```bash
# Unified Processor (via HTTP)
DOC_UNI_PROC_URL=http://localhost:8090
DOC_UNI_PROC_TIMEOUT_SECS=180

# Service
PORT=8080
ENVIRONMENT=development
```

### HTTP Integration

This service sends document content to unified-processor via HTTP POST for chunking and embedding.

## Development

### Dependencies

The service requires Python 3.8+ with the following key dependencies:

**Core Framework:**
- FastAPI (≥0.109.0): Web framework
- Uvicorn: ASGI server
- Pydantic (≥2.5.0): Data validation

**HTTP Communication:**
- httpx: Async HTTP client
- Requests: HTTP library

**Other:**
- aiofiles: Async file I/O

### How to run the microservice Locally
```bash
# Install dependencies
pip install -e .

# Set environment
export ENVIRONMENT=development
export DOC_UNI_PROC_URL=http://localhost:8090

# Run service
python -m app.main
```

### Testing
```bash
# Run tests
pytest tests/
```

## Deployment

### Docker
```bash
docker build -t confuse/data-connector .
docker run -p 8080:8080 confuse/data-connector
```

### Kubernetes
```bash
kubectl apply -f k8s/data-connector.yaml
```
