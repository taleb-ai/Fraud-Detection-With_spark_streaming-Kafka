"""Command-line entry point for the banking transaction generator.

Run with `python -m generator.main --small` for a quick test mode.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from .user_generator import UserGenerator
from .transaction_generator import TransactionGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Banking transactions generator")
    p.add_argument("--small", action="store_true", help="Run in small test mode (N=100, M=200, TPS=10)")
    p.add_argument("--duration", type=int, default=10, help="Simulation duration in seconds")
    p.add_argument("--tps", type=float, default=None, help="Target transactions per second")
    p.add_argument("--out", type=str, default="data/transactions.json", help="Output file for transactions")
    p.add_argument("--kafka", action="store_true", help="Send transactions to Kafka (requires Kafka running)")
    p.add_argument("--kafka-bootstrap", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--kafka-topic", type=str, default="transactions", help="Kafka topic name")
    return p.parse_args()


def main():
    args = parse_args()

    if args.small:
        N = 100
        M = 200
        tps = args.tps if args.tps is not None else 10.0
    else:
        N = 100000
        M = 200000
        tps = args.tps if args.tps is not None else 1000.0

    logger.info("Starting generator: N=%d, M=%d, duration=%ds, tps=%s, kafka=%s",
                N, M, args.duration, tps, args.kafka)

    ug = UserGenerator(N=N, M=M)
    users = ug.generate_users()
    ug.print_statistics()

    tg = TransactionGenerator(users)
    start_time = datetime.utcnow()

    # Handle Kafka mode
    kafka_producer = None
    if args.kafka:
        try:
            from .kafka_producer import KafkaTransactionProducer
            logger.info("Initializing Kafka producer...")
            kafka_producer = KafkaTransactionProducer(bootstrap_servers=args.kafka_bootstrap,
                                                       topic=args.kafka_topic)
            if not kafka_producer.test_connection():
                logger.warning("Kafka connection test failed, disabling Kafka mode")
                kafka_producer = None
        except ImportError:
            logger.error("confluent-kafka not installed. Run: pip install confluent-kafka")
            logger.warning("Falling back to regular simulation without Kafka")
        except Exception as e:
            logger.error("Failed to initialize Kafka: %s", e)
            logger.warning("Falling back to regular simulation without Kafka")

    # Run simulation
    try:
        if kafka_producer is not None:
            logger.info("Running simulation with Kafka streaming")
            txs = tg.run_simulation_with_kafka(duration_seconds=args.duration, target_tps=float(tps),
                                               kafka_producer=kafka_producer, verbose=True,
                                               start_time=start_time)
        else:
            logger.info("Running regular simulation")
            txs = tg.run_simulation(duration_seconds=args.duration, target_tps=float(tps),
                                    verbose=True, start_time=start_time)
    finally:
        if kafka_producer is not None:
            kafka_producer.close()

    # Always save to file
    tg.save_transactions(txs, args.out)

    logger.info("Done. Transactions saved to %s", args.out)


if __name__ == "__main__":
    main()
