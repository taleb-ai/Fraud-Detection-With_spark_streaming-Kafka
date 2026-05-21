"""Kafka producer for banking transactions.

Provides `KafkaTransactionProducer` which sends transactions to a Kafka topic
in real-time with retry logic and delivery callbacks.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaTransactionProducer:
    """Send transactions to a Kafka topic.

    Parameters
    ----------
    bootstrap_servers: str
        Comma-separated Kafka broker addresses (default: 'localhost:9092')
    topic: str
        Kafka topic name for transactions (default: 'transactions')
    """

    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'transactions') -> None:
        if not KAFKA_AVAILABLE:
            raise ImportError("confluent-kafka not installed. Run: pip install confluent-kafka")

        self.topic = topic
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'delivery.timeout.ms': 5000,
            'compression.type': 'snappy',
        }

        try:
            self.producer = Producer(self.producer_config)
            logger.info("Kafka producer initialized for topic '%s' on %s", topic, bootstrap_servers)
        except Exception as e:
            logger.error("Failed to initialize Kafka producer: %s", e)
            raise

    def delivery_callback(self, err, msg) -> None:
        """Callback for message delivery success/failure.

        Parameters
        ----------
        err: KafkaError or None
            Error if delivery failed, None otherwise.
        msg: Message
            Message object with partition, offset info.
        """
        if err:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.debug("Message delivered to topic %s [partition %d, offset %d]",
                         msg.topic(), msg.partition(), msg.offset())

    def send_transaction(self, transaction: dict) -> None:
        """Send a single transaction to Kafka.

        Parameters
        ----------
        transaction: dict
            Transaction dict with fields: transaction_id, sender_id, amount, etc.

        Raises
        ------
        ValueError
            If transaction is invalid.
        """
        if not isinstance(transaction, dict):
            raise ValueError("transaction must be a dict")

        try:
            sender_id = transaction.get("sender_id", "unknown")
            msg_value = json.dumps(transaction).encode('utf-8')
            msg_key = sender_id.encode('utf-8')

            self.producer.produce(
                topic=self.topic,
                key=msg_key,
                value=msg_value,
                callback=self.delivery_callback
            )
        except Exception as e:
            logger.error("Error sending transaction: %s", e)
            raise

    def send_batch(self, transactions: list) -> int:
        """Send a batch of transactions to Kafka.

        Parameters
        ----------
        transactions: list of dict
            List of transaction dicts.

        Returns
        -------
        int
            Number of transactions sent successfully.
        """
        if not isinstance(transactions, list):
            raise ValueError("transactions must be a list")

        count = 0
        for tx in transactions:
            try:
                self.send_transaction(tx)
                count += 1
            except Exception as e:
                logger.warning("Failed to send one transaction: %s", e)

        # Flush to ensure delivery
        try:
            self.producer.flush(timeout=5)
            logger.debug("Flushed %d transactions to Kafka", count)
        except Exception as e:
            logger.error("Error during flush: %s", e)

        return count

    def close(self) -> None:
        """Close the Kafka producer connection."""
        try:
            self.producer.flush(timeout=5)
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error("Error closing producer: %s", e)

    def test_connection(self) -> bool:
        """Test Kafka connection by sending a test message.

        Returns
        -------
        bool
            True if connection test succeeded, False otherwise.
        """
        try:
            test_msg = {
                "test": True,
                "message": "Connection test from banking fraud detector"
            }
            self.send_transaction(test_msg)
            self.producer.flush(timeout=2)
            logger.info("✓ Kafka connection test successful")
            return True
        except Exception as e:
            logger.error("✗ Kafka connection test failed: %s", e)
            logger.error("  Make sure Kafka is running: docker-compose up -d")
            logger.error("  Check connection at: http://localhost:8080 (Kafka UI)")
            return False
