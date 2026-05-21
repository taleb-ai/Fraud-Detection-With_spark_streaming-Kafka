"""Test script for Kafka connection.

Run with: python test_kafka_connection.py

This script tests whether Kafka is running and accessible.
"""
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logger.info("Testing Kafka connection...")

    try:
        from generator.kafka_producer import KafkaTransactionProducer
    except ImportError as e:
        logger.error("Failed to import KafkaTransactionProducer: %s", e)
        logger.error("Make sure you have installed dependencies: pip install -r requirements.txt")
        return False

    try:
        producer = KafkaTransactionProducer(bootstrap_servers='localhost:9092', topic='transactions')
        success = producer.test_connection()
        producer.close()

        if success:
            logger.info("\n✓ Kafka is running and accessible!")
            logger.info("  You can now run: python -m generator.main --small --kafka")
            logger.info("  Monitor transactions at: http://localhost:8080")
            return True
        else:
            logger.error("\n✗ Kafka connection failed")
            logger.error("\nNext steps:")
            logger.error("  1. Start Kafka: docker-compose up -d")
            logger.error("  2. Wait 30s for Kafka to start")
            logger.error("  3. Run this test again")
            logger.error("  4. Monitor at: http://localhost:8080")
            return False

    except Exception as e:
        logger.error("Unexpected error: %s", e)
        logger.error("\nDebug steps:")
        logger.error("  1. Check Docker is running: docker ps")
        logger.error("  2. Check Kafka logs: docker-compose logs kafka")
        logger.error("  3. Verify port 9092 is open: netstat -an | grep 9092")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
