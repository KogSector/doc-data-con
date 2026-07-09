"""
Kafka event infra for Data Connector.

This module contains the low-level EventProducer initialization and lifecycle
management (connection, flush/close).
"""

import structlog
from typing import Optional
from .kafka_client import EventProducer, KafkaConfig
from .events import *
from .topics import Topics
from app.config import get_settings

logger = structlog.get_logger()

_producer: Optional[EventProducer] = None


def init_event_producer() -> Optional[EventProducer]:
    """Initialize the global Kafka event producer and return it.

    Raises an exception if Kafka is not available (service cannot start without it).
    """
    global _producer

    settings = get_settings()

    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        import time

        config = KafkaConfig.from_env()
        _producer = EventProducer(config=config)

        # Standardized robust health check
        _producer.wait_until_ready(timeout=300)

        # Access underlying producer to trigger any connection setup/errors
        _ = _producer.producer

        logger.info(
            "Kafka event producer initialized",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
        )

        # Explicitly create topics to avoid UNKNOWN_TOPIC_OR_PART errors
        try:
            from confluent_kafka.admin import AdminClient, NewTopic

            admin = AdminClient(config.to_producer_config())
            topics_to_create = [
                NewTopic(Topics.REPO_EVENTS, num_partitions=1, replication_factor=1)
            ]
            fs = admin.create_topics(topics_to_create)
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info("Kafka topic created explicitly", topic=topic)
                except Exception as e:
                    if "TOPIC_ALREADY_EXISTS" not in str(e):
                        logger.debug("Topic creation info", topic=topic, msg=str(e))
        except Exception as e:
            logger.warning("Failed to explicitly create topics", error=str(e))

        return _producer
    except Exception as e:
        logger.error("Failed to initialize Kafka event producer", error=str(e))
        raise RuntimeError(f"Kafka is required but not available: {e}") from e


def get_event_producer() -> Optional[EventProducer]:
    """Return the initialized EventProducer or None."""
    return _producer


def close_event_producer() -> None:
    """Flush and clear the global producer if present."""
    global _producer
    if _producer:
        logger.info("Closing Kafka event producer")
        try:
            _producer.flush()
        except Exception:
            logger.exception("Error flushing producer during close")
        _producer = None
