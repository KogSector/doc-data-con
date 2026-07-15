"""
HTTP Document Processor - Replaces Kafka streaming
"""

import httpx
import structlog
import asyncio
import time
import base64
from typing import Dict, Any, Tuple
from app.config import get_settings

logger = structlog.get_logger()

class HttpDocumentProcessor:
    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.doc_uni_proc_url
        self.timeout = getattr(self.settings, 'doc_uni_proc_timeout_secs', 180)
        self.retry_attempts = getattr(self.settings, 'doc_uni_proc_retry_attempts', 3)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            limits=httpx.Limits(max_keepalive_connections=10, keepalive_expiry=30.0)
        )

    async def process_document(self, source_id: str, file_id: str, filename: str, content: bytes | str, metadata: Dict[str, Any], user_id: str = "system") -> Tuple[bool, int, int, str]:
        """
        Sends document directly to unified-processor over HTTP.
        Returns: (success, chunk_count, processing_time_ms, error)
        """
        start_time = time.time()
        
        if isinstance(content, str):
            b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        else:
            b64_content = base64.b64encode(content).decode("utf-8")

        payload = {
            "content": b64_content,
            "is_base64": True,
            "filename": filename,
            "source_id": source_id,
            "file_id": file_id,
            "user_id": user_id,
            "metadata": metadata
        }

        endpoint = f"{self.base_url.rstrip('/')}/api/v1/documents/process"
        headers = {"X-API-Key": self.settings.internal_api_key}
        
        for attempt in range(self.retry_attempts):
            try:
                response = await self.client.post(endpoint, json=payload, headers=headers)
                
                if response.status_code in (200, 201, 202):
                    data = response.json() if response.content else {}
                    processing_time_ms = int((time.time() - start_time) * 1000)
                    chunk_count = data.get("chunk_count", 0)
                    logger.info("Successfully processed document via HTTP", filename=filename, source_id=source_id, time_ms=processing_time_ms)
                    return True, chunk_count, processing_time_ms, ""
                
                if response.status_code >= 500:
                    if attempt < self.retry_attempts - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s", filename=filename)
                        await asyncio.sleep(wait_time)
                        continue
                        
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error("Failed to process document", filename=filename, error=error_msg)
                return False, 0, int((time.time() - start_time) * 1000), error_msg
                
            except httpx.RequestError as e:
                if attempt < self.retry_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request error {str(e)}, retrying in {wait_time}s", filename=filename)
                    await asyncio.sleep(wait_time)
                    continue
                error_msg = f"Request error: {str(e)}"
                logger.error("Failed to process document due to request error", filename=filename, error=error_msg)
                return False, 0, int((time.time() - start_time) * 1000), error_msg

        return False, 0, int((time.time() - start_time) * 1000), "Max retries exceeded"

    async def close(self):
        await self.client.aclose()

_processor_instance = None

def get_document_processor() -> HttpDocumentProcessor:
    global _processor_instance
    if _processor_instance is None:
        _processor_instance = HttpDocumentProcessor()
    return _processor_instance
