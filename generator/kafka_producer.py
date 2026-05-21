"""Kafka producer for banking transactions (TP5 §5.1)."""
from __future__ import annotations

import json
import logging
import time

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from .config import DEFAULT_KAFKA_BOOTSTRAP, DEFAULT_KAFKA_TOPIC

logger = logging.getLogger(__name__)


def _normalize_bootstrap(servers: str) -> str:
    """Prefer IPv4 loopback to avoid localhost -> ::1 connection refused."""
    return servers.replace("localhost:", "127.0.0.1:")


class KafkaTransactionProducer:
    """Send bank-format transactions to a Kafka topic."""

    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None) -> None:
        bootstrap_servers = _normalize_bootstrap(bootstrap_servers or DEFAULT_KAFKA_BOOTSTRAP)
        topic = topic or DEFAULT_KAFKA_TOPIC
        if not KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka not installed. Run: pip install confluent-kafka")

        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self._delivery_errors: list = []
        self._delivered_count = 0
        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "delivery.timeout.ms": 10000,
            "compression.type": "snappy",
            "socket.timeout.ms": 10000,
            "broker.address.family": "v4",
            "socket.connection.setup.timeout.ms": 10000,
        }

        self.producer = Producer(self.producer_config)
        logger.info("Kafka producer initialized for topic '%s' on %s", topic, bootstrap_servers)

    def delivery_callback(self, err, msg) -> None:
        if err:
            self._delivery_errors.append(err)
            logger.error("Message delivery failed: %s", err)
        else:
            self._delivered_count += 1
            logger.debug(
                "Delivered to %s [partition %d, offset %d]",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send_transaction(self, transaction: dict) -> None:
        if not isinstance(transaction, dict):
            raise ValueError("transaction must be a dict")

        key = transaction.get("send_id") or transaction.get("sender_id", "unknown")
        msg_value = json.dumps(transaction).encode("utf-8")
        self.producer.produce(
            topic=self.topic,
            key=str(key).encode("utf-8"),
            value=msg_value,
            callback=self.delivery_callback,
        )

    def send_batch(self, transactions: list) -> int:
        if not isinstance(transactions, list):
            raise ValueError("transactions must be a list")

        before_errors = len(self._delivery_errors)
        delivered_before = self._delivered_count
        for tx in transactions:
            self.send_transaction(tx)

        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages still in producer queue after flush", remaining)

        new_errors = len(self._delivery_errors) - before_errors
        delivered = self._delivered_count - delivered_before
        if new_errors > 0 or delivered < len(transactions):
            logger.error(
                "Kafka delivery incomplete: delivered=%d/%d errors=%d",
                delivered,
                len(transactions),
                new_errors,
            )
            return delivered
        return len(transactions)

    def close(self) -> None:
        self.producer.flush(timeout=10)
        logger.info("Kafka producer closed")

    def _check_brokers(self) -> bool:
        admin = AdminClient(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "broker.address.family": "v4",
                "socket.connection.setup.timeout.ms": 10000,
            }
        )
        metadata = admin.list_topics(timeout=10)
        if metadata is None or not metadata.brokers:
            logger.error("No Kafka brokers reachable at %s", self.bootstrap_servers)
            return False
        logger.info(
            "Cluster metadata OK: %d broker(s), %d topic(s)",
            len(metadata.brokers),
            len(metadata.topics),
        )
        return True

    def _try_deliver_test_message(self) -> bool:
        self._delivery_errors.clear()
        delivered_before = self._delivered_count

        test_msg = {
            "msg_entity": "bank_X",
            "app_type": "mobile_app",
            "send_entity": "bank_X",
            "receive_entity": "bank_X",
            "send_id": "__connection_test__",
            "receive_id": "__connection_test__",
            "amount": 0.01,
            "date": "2025-01-01T00:00:00Z",
            "tx_type": "transfer",
            "tx_id": "__connection_test__",
        }

        self.send_transaction(test_msg)
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.error("Flush timed out with %d message(s) pending", remaining)
            return False
        if self._delivery_errors:
            logger.error("Delivery failed: %s", self._delivery_errors[-1])
            return False
        if self._delivered_count <= delivered_before:
            logger.error("Test message was not acknowledged by the broker")
            return False
        return True

    def test_connection(self, retries: int = 3, retry_delay: float = 2.0) -> bool:
        """Verify broker reachability and successful message delivery."""
        for attempt in range(1, retries + 1):
            if attempt > 1:
                logger.info("Kafka connection retry %d/%d ...", attempt, retries)
                time.sleep(retry_delay)

            try:
                if not self._check_brokers():
                    continue
                if self._try_deliver_test_message():
                    logger.info("Kafka connection test successful")
                    return True
            except Exception as e:
                logger.error("Kafka connection attempt failed: %s", e)

        logger.error(
            "Kafka not ready at %s — run: docker compose up -d kafka && sleep 15",
            self.bootstrap_servers,
        )
        return False
