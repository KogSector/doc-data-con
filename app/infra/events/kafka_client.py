from threading import Event
from confluent_kafka import Producer
import os
from dataclasses import dataclass
import logging
from pydantic import BaseModel
from typing import Optional, Any
from confluent_kafka import Consumer, KafkaError
from typing import Protocol, List

"""
Unified Kafka Client for Confluent Cloud

Handles config, producing, and consuming.
"""


logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration error"""

    pass


@dataclass
class KafkaConfig:
    """
    Kafka configuration for Confluent Cloud

    Requires CONFLUENT_* environment variables for configuration:
    - CONFLUENT_BOOTSTRAP_SERVERS: Confluent Cloud bootstrap servers
    - CONFLUENT_API_KEY: SASL username (Confluent Cloud API key)
    - CONFLUENT_API_SECRET: SASL password (Confluent Cloud API secret)
    - KAFKA_CLIENT_ID: Client ID for this service
    - KAFKA_GROUP_ID: Consumer group ID (for consumers)
    """

    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_ca_pem: Optional[str] = None
    client_id: str = "confuse-service"
    group_id: Optional[str] = None
    enable_idempotence: bool = True

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """
        Create a new KafkaConfig from Pydantic settings.
        """
        from app.config import get_settings

        settings = get_settings()

        config = cls(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            security_protocol=settings.kafka_security_protocol,
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_username=settings.kafka_sasl_username,
            sasl_password=settings.kafka_sasl_password,
            ssl_ca_pem=settings.kafka_ssl_ca_pem,
            client_id=settings.kafka_client_id,
            group_id=os.getenv("KAFKA_GROUP_ID"),
            enable_idempotence=settings.kafka_enable_idempotence,
        )

        # Log configuration (without secrets)
        logger.info(
            f"Kafka config: bootstrap_servers={config.bootstrap_servers}, "
            f"security={config.security_protocol}, client_id={config.client_id}"
        )

        return config

    def to_producer_config(self) -> dict:
        """Build a confluent-kafka producer configuration dict"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 100,
            "request.timeout.ms": 30000,
            "enable.idempotence": self.enable_idempotence,
            "broker.address.family": "v4",
        }

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            config["sasl.username"] = self.sasl_username

        if self.sasl_password:
            config["sasl.password"] = self.sasl_password

        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location

        if self.ssl_ca_pem:
            config["ssl.ca.pem"] = self.ssl_ca_pem.replace("\\n", "\n")

        return config

    def to_consumer_config(self) -> dict:
        """Build a confluent-kafka consumer configuration dict"""
        if not self.group_id:
            raise ConfigError("KAFKA_GROUP_ID is required for consumers")

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "group.id": self.group_id,
            "security.protocol": self.security_protocol,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45000,
            "broker.address.family": "v4",
        }

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            config["sasl.username"] = self.sasl_username

        if self.sasl_password:
            config["sasl.password"] = self.sasl_password

        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location

        if self.ssl_ca_pem:
            config["ssl.ca.pem"] = self.ssl_ca_pem.replace("\\n", "\n")

        return config

    def validate(self) -> None:
        """Validate the configuration"""
        if not self.bootstrap_servers:
            raise ConfigError("bootstrap_servers cannot be empty")

        if self.security_protocol != "PLAINTEXT" and (
            not self.sasl_username or not self.sasl_password
        ):
            raise ConfigError("SASL credentials are required for authenticated Kafka")


logger = logging.getLogger(__name__)


class ConsumerError(Exception):
    """Consumer error"""

    pass


class EventHandler(Protocol):
    """Protocol for event handlers"""

    def handle(self, topic: str, payload: bytes) -> None:
        """
        Handle a raw message from Kafka

        Implementations should deserialize the payload and process the event.
        """
        ...

    def handle_error(self, topic: str, error: Exception, payload: Optional[bytes] = None) -> None:
        """
        Handle deserialization or processing errors

        Default implementations can send to DLQ.
        """
        ...


class EventConsumer:
    """Event consumer for subscribing to Kafka topics"""

    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Create a new event consumer

        Args:
            config: Kafka configuration. If None, will be loaded from environment.
        """
        self.config = config or KafkaConfig.from_env()
        self._consumer: Optional[Consumer] = None
        self._shutdown = Event()

    @classmethod
    def from_env(cls) -> "EventConsumer":
        """Create a new event consumer from environment configuration"""
        return cls(KafkaConfig.from_env())

    @property
    def consumer(self) -> Consumer:
        """Get or create the underlying Kafka consumer"""
        if self._consumer is None:
            consumer_config = self.config.to_consumer_config()
            self._consumer = Consumer(consumer_config)
            logger.info(
                f"Created Kafka consumer for {self.config.bootstrap_servers} "
                f"(group: {self.config.group_id})"
            )
        return self._consumer

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to one or more topics"""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def run(self, handler: EventHandler, poll_timeout: float = 1.0) -> None:
        """
        Start consuming messages with the provided handler

        This method runs until shutdown is called or an unrecoverable error occurs.

        Args:
            handler: EventHandler implementation
            poll_timeout: Timeout for polling in seconds
        """
        logger.info("Starting consumer loop")

        try:
            while not self._shutdown.is_set():
                msg = self.consumer.poll(poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"End of partition reached: {msg.topic()} "
                            f"[{msg.partition()}] @ {msg.offset()}"
                        )
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    handler.handle(msg.topic(), msg.value())

                    # Commit offset after successful processing
                    self.consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    handler.handle_error(msg.topic(), e, msg.value())

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
        finally:
            self.close()

    def shutdown(self) -> None:
        """Signal the consumer to shut down"""
        logger.info("Shutdown requested")
        self._shutdown.set()

    def close(self) -> None:
        """Close the consumer"""
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer closed")

    def __enter__(self) -> "EventConsumer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def deserialize_event(event_class: type[BaseModel], payload: bytes) -> BaseModel:
    """
    Helper function to deserialize a message payload

    Args:
        event_class: Pydantic model class to deserialize into
        payload: Raw bytes from Kafka

    Returns:
        Deserialized event
    """
    return event_class.model_validate_json(payload)


logger = logging.getLogger(__name__)


class ProducerError(Exception):
    """Producer error"""

    pass


class EventProducer:
    """Event producer for publishing events to Kafka"""

    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Create a new event producer

        Args:
            config: Kafka configuration. If None, will be loaded from environment.
        """
        self.config = config or KafkaConfig.from_env()
        self._producer: Optional[Producer] = None

    @classmethod
    def from_env(cls) -> "EventProducer":
        """Create a new event producer from environment configuration"""
        return cls(KafkaConfig.from_env())

    @property
    def producer(self) -> Producer:
        """Get or create the underlying Kafka producer"""
        if self._producer is None:
            producer_config = self.config.to_producer_config()
            self._producer = Producer(producer_config)
            logger.info(
                f"Created Kafka producer for {self.config.bootstrap_servers} "
                f"({self.config.client_id})"
            )
        return self._producer

    def wait_until_ready(self, timeout: int = 300) -> None:
        """
        Wait synchronously until Kafka is fully available.
        Uses AdminClient to fetch cluster metadata.
        """
        from confluent_kafka.admin import AdminClient
        import time

        logger.info(
            f"Waiting up to {timeout}s for Kafka to be ready at {self.config.bootstrap_servers}..."
        )
        start_time = time.time()
        admin_config = self.config.to_producer_config()

        admin_client = AdminClient(admin_config)

        attempt = 1
        while True:
            try:
                metadata = admin_client.list_topics(timeout=5.0)
                if metadata and metadata.brokers:
                    logger.info("Kafka is healthy and ready to accept connections.")
                    return
            except Exception as e:
                logger.debug(f"Kafka health check attempt {attempt} failed: {e}")

            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise ProducerError(f"Kafka failed to become ready after {timeout} seconds.")

            attempt += 1
            time.sleep(2.0)

    def publish(self, event: BaseModel) -> None:
        """
        Publish an event to its designated topic

        The event must be a Pydantic model with a static topic() method.
        """
        topic = event.topic() if hasattr(event, "topic") else "unknown"
        self.publish_to_topic(event, topic)

    def publish_to_topic(
        self,
        event: BaseModel,
        topic: str,
        key: Optional[str] = None,
    ) -> None:
        """
        Publish an event to a specific topic

        Args:
            event: Pydantic model to publish
            topic: Kafka topic name
            key: Optional message key
        """
        payload = event.model_dump_json()

        logger.debug(f"Publishing event to topic '{topic}': {len(payload)} bytes")

        self.producer.produce(
            topic=topic,
            value=payload.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            callback=self._delivery_callback,
        )

        # Trigger delivery reports
        self.producer.poll(0)

    def publish_with_retry_to_topic(
        self,
        event: BaseModel,
        topic: str,
        key: Optional[str] = None,
        retries: int = 3,
        dlq_topic: Optional[str] = None,
    ) -> None:
        """
        Publish an event with retries and optional DLQ fallback.
        Retries use exponential backoff starting at 0.5s.
        """
        last_err: Optional[Exception] = None

        for attempt in range(retries):
            try:
                self.publish_to_topic(event, topic, key=key)
                # Give producer a chance to process delivery report
                self.producer.poll(0)
                return
            except Exception as e:
                last_err = e
                delay = (2**attempt) * 0.5
                logger.warning(
                    f"Publish attempt {attempt + 1} failed for {topic}, retrying in {delay}s: {e}"
                )
                import time

                time.sleep(delay)

        logger.error(f"Failed to publish event to {topic} after {retries} attempts: {last_err}")

        final_dlq = dlq_topic or (f"{topic}.dlq" if topic else None)
        if final_dlq:
            try:
                envelope = {
                    "failedTopic": topic,
                    "failedAt": int(__import__("time").time() * 1000),
                    "error": str(last_err),
                    "event": event.model_dump(),
                }
                # Use publish_raw to send JSON envelope
                import json

                self.publish_raw(final_dlq, json.dumps(envelope), key=key)
                self.producer.poll(0)
                logger.info(f"Published failure envelope to DLQ {final_dlq}")
            except Exception as dlq_err:
                logger.exception("Failed to publish to DLQ", exc_info=dlq_err)

        # Raise original error for caller handling
        if last_err:
            raise ProducerError(str(last_err))

    def publish_raw(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
    ) -> None:
        """
        Publish a raw string value to a topic

        Args:
            topic: Kafka topic name
            value: Raw string value
            key: Optional message key
        """
        self.producer.produce(
            topic=topic,
            value=value.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """
        Wait for all buffered messages to be delivered

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue (0 if all delivered)
        """
        return self.producer.flush(timeout)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback for delivery reports"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def __enter__(self) -> "EventProducer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.flush()
