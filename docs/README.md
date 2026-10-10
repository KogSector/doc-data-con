# Document Data Connector (doc-data-con)

**Port**: 8081 · **Role**: Document source integration and intelligent file
routing (document flavour of the data connector)

> This is the canonical README for the service; **all documentation lives in
> [`docs/`](./)** (this folder). See [index.md](index.md) for the docs index.

## Overview

`doc-data-con` is the entry point for **document** sources in the ConFuse
platform. It handles:

- **Source Management**: Connect to Notion, Figma, URLs, cloud document storage
- **File Type Detection**: Analyze files to determine if they're code or documents
- **Intelligent Routing**: Route files to the appropriate processors
- **Document Processing**: Send documents to `doc-uni-proc` for chunking and embedding
- **Webhook Handling**: Receive triggers from external systems

> The repository flavour lives in `repo-data-con` (clone/sync + Kafka events).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Document Data Connector (:8081)             │
├─────────────────────────────────────────────────────────────┤
│  Source Management │ File Classification │ HTTP Client      │
└────────────────────┴─────────────────────┴──────────────────┘
                             │
                             ▼
                   ┌──────────────────┐
                   │  doc-uni-proc    │
                   │  (documents)     │
                   └──────────────────┘
```

## Supported Sources

- **Document storage**: Notion, Figma, Google Drive, OneDrive, Dropbox
- **Web**: arbitrary URLs (`/api/v1/external/urls`)
- **File types**: PDF, Word, Markdown, Text; YAML/JSON/TOML configuration

## Key API surface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health`, `/health/detailed` | GET | Health checks |
| `/api/sources` | GET/POST | List / create document sources |
| `/api/sources/{id}` | GET/PUT/DELETE | Source CRUD |
| `/api/sources/{id}/sync` | POST | Trigger sync |
| `/api/documents*` | — | Document records & analytics |
| `/api/v1/external/urls` | GET/POST/PUT/DELETE | URL ingestion |
| `/api/v1/ingest` | POST | Start ingestion |
| `/api/v1/jobs` | GET | Ingestion job status |
| `/api/v1/figma*`, `/api/v1/notion*` | — | Provider connectors |

Full schemas: [api-reference.md](api-reference.md).

## Configuration

```bash
# Document processor (via HTTP)
DOC_UNI_PROC_URL=http://localhost:8090
DOC_UNI_PROC_TIMEOUT_SECS=180

# Service
PORT=8081
ENVIRONMENT=development
```

## How to run the microservice

```bash
# Install dependencies
pip install -e .

# Set environment
export ENVIRONMENT=development
export DOC_UNI_PROC_URL=http://localhost:8090

# Run service
python -m app.main
```

## Testing

Tests for this service live in the central test module (this service contains
no tests):

```bash
cd ../ConFuse-test-module
pip install -r requirements.txt
pytest data_connector -v
```

## Deployment

```bash
# Docker
docker build -t confuse/doc-data-con .
docker run -p 8081:8081 confuse/doc-data-con
```

## Documentation

| Document | Contents |
|----------|----------|
| [index.md](index.md) | Overview & docs index |
| [api-reference.md](api-reference.md) | Full endpoint reference |
| [architecture.md](architecture.md) | Internal architecture |
| [connectors.md](connectors.md) | Provider connector details |
| [setup.md](setup.md) | Local development setup |
