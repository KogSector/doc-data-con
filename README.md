# Data Connector Service

**Port**: 8080  
**Role**: Universal source integration and intelligent file routing

## Overview

The Data Connector service is the entry point for all data sources in the ConFuse platform. It handles:

- **Source Management**: Connect to Git repositories, document storage, APIs
- **File Type Detection**: Analyze files to determine if they're code or documents
- **Intelligent Routing**: Route files to appropriate processors via Kafka
- **Event Production**: Publish file ingestion events to Confluent Cloud
- **Webhook Handling**: Receive triggers from external systems

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Connector (:8080)                    │
├─────────────────────────────────────────────────────────────┤
│  Source Management  │  File Classification  │  Events       │
└─────────────────────┴──────────────────────┴───────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Confluent Cloud │
                    │     Kafka        │
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

## Event Flow

### File Ingestion
```
Repository Sync → File Discovery → Classification → Event Production
```

### Events Produced
```
code.ingested      # For code files
docs.ingested      # For document files
source.sync.completed
source.sync.failed
```

## Configuration

### Environment Variables
```bash
# Unified Processor (via Kafka)
# Data-Connector publishes ingestion events to Kafka; Unified-Processor consumes them.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_DLQ_TOPIC=confuse-dlq

# Confluent Cloud (optional)
CONFLUENT_BOOTSTRAP_SERVERS=pkc-7prvp.centralindia.azure.confluent.cloud:9092
CONFLUENT_API_KEY=your_api_key
CONFLUENT_API_SECRET=your_api_secret

# Service
PORT=8080
ENVIRONMENT=development
```

### Kafka Integration

This service publishes ingestion events to Kafka which the Unified Processor consumes. Use the
`KAFKA_BOOTSTRAP_SERVERS` env var to point to your Kafka cluster. For production, set `KAFKA_DLQ_TOPIC`
to capture messages that exhaust retries.

## Development

### Dependencies

The service requires Python 3.8+ with the following key dependencies:

**Core Framework:**
- FastAPI (≥0.109.0): Web framework
- Uvicorn: ASGI server
- Pydantic (≥2.5.0): Data validation

**gRPC Communication (v1.60.0+):**
- grpcio: Core gRPC runtime
- grpcio-tools: Protocol buffer compiler
- grpcio-status: Rich error status codes
- grpcio-reflection: Service reflection support

**Other:**
- httpx: Async HTTP client
- GitPython: Git repository operations
- aiofiles: Async file I/O

### Running Locally
```bash
# Install dependencies
pip install -e .

# Set environment
export ENVIRONMENT=development
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run service
python -m app.main
```

### Testing
```bash
# Run tests
pytest tests/

# Test Kafka connectivity
python -m app.kafka.test_connection
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
