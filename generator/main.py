"""Command-line entry point for the banking transaction generator (TP5 §5.1).

Examples::

    # Laptop-friendly test (runs ~30s then exits automatically)
    python -m generator.main --test

    # Same test with Kafka
    python -m generator.main --test --kafka

    # Full committee demo (heavy — 300k users, ~1000 tx/s, Ctrl+C to stop)
    python -m generator.main --kafka --peak-hour

    # Manual short run
    python -m generator.main --small --kafka --duration 10 --no-real-time
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
from datetime import datetime, timezone

from .config import (
    DEFAULT_KAFKA_BOOTSTRAP,
    DEFAULT_KAFKA_TOPIC,
    PEAK_TARGET_TPS,
    TEST_DURATION_SEC,
    TEST_M,
    TEST_N,
    TEST_TARGET_TPS,
)
from .transaction_generator import TransactionGenerator
from .user_generator import UserGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown_requested = False


def _handle_shutdown(signum, frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Shutdown signal received (%s), finishing current second...", signum)


def parse_args():
    p = argparse.ArgumentParser(description="Banking transactions generator (TP5 §5.1)")
    p.add_argument(
        "--test",
        action="store_true",
        help=(
            f"Light test mode: {TEST_N}+{TEST_M} users, ~{TEST_TARGET_TPS:.0f} tx/s, "
            f"{TEST_DURATION_SEC}s then exit (no peak-hour, fast clock)"
        ),
    )
    p.add_argument("--small", action="store_true", help="Small population (N=100, M=200)")
    p.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Run for N seconds then exit (0 = infinite until Ctrl+C; --test sets 30)",
    )
    p.add_argument(
        "--prob-scale",
        type=float,
        default=1.0,
        help="Multiply per-user probabilities (stress testing)",
    )
    p.add_argument(
        "--peak-hour",
        action="store_true",
        help=f"Scale probabilities to ~{PEAK_TARGET_TPS:.0f} tx/s peak load (heavy)",
    )
    p.add_argument(
        "--fraud-rate",
        type=float,
        default=0.0,
        help="Inject simulated fraud txs (fraction of batch size, e.g. 0.001)",
    )
    p.add_argument(
        "--no-real-time",
        action="store_true",
        help="Do not sleep 1s between seconds (fast-forward simulation)",
    )
    p.add_argument(
        "--out",
        type=str,
        default="",
        help="Save transactions to JSON when duration > 0",
    )
    p.add_argument("--kafka", action="store_true", help="Stream transactions to Kafka")
    p.add_argument(
        "--no-kafka",
        action="store_true",
        help="Disable Kafka even in --test mode",
    )
    p.add_argument(
        "--kafka-bootstrap",
        type=str,
        default=DEFAULT_KAFKA_BOOTSTRAP,
        help="Kafka bootstrap (host: 127.0.0.1:9094; Docker: kafka:9092)",
    )
    p.add_argument("--kafka-topic", type=str, default=DEFAULT_KAFKA_TOPIC, help="Kafka topic")
    p.add_argument("-v", "--verbose", action="store_true", help="More frequent progress logs")
    return p.parse_args()


def _resolve_run_settings(args):
    """Return (n, m, duration, real_time, use_kafka, prob_scale, fraud_rate, out_path)."""
    test_mode = args.test
    use_kafka = args.kafka and not args.no_kafka

    if test_mode:
        n, m = TEST_N, TEST_M
        duration = args.duration if args.duration > 0 else TEST_DURATION_SEC
        real_time = False
        use_kafka = use_kafka or not args.no_kafka  # default Kafka on in test mode
        prob_scale = args.prob_scale
        fraud_rate = args.fraud_rate if args.fraud_rate > 0 else 0.01
        out_path = args.out or "data/transactions_test.json"
        peak_hour = False
        logger.info(
            "TEST MODE: %d users, %ds wall-clock simulation, kafka=%s, then exit",
            n + m,
            duration,
            use_kafka,
        )
    elif args.small:
        n, m = 100, 200
        duration = args.duration
        real_time = not args.no_real_time
        prob_scale = args.prob_scale
        fraud_rate = args.fraud_rate
        out_path = args.out
        peak_hour = args.peak_hour
    else:
        n, m = 100_000, 200_000
        duration = args.duration
        real_time = not args.no_real_time
        prob_scale = args.prob_scale
        fraud_rate = args.fraud_rate
        out_path = args.out
        peak_hour = args.peak_hour

    if peak_hour and not test_mode:
        pass  # scaled later after TransactionGenerator init
    elif test_mode and args.peak_hour:
        logger.warning("--peak-hour ignored in --test mode (use full run without --test)")

    return n, m, duration, real_time, use_kafka, prob_scale, fraud_rate, out_path, peak_hour, test_mode


def main() -> int:
    global _shutdown_requested
    args = parse_args()
    (
        n,
        m,
        duration,
        real_time,
        use_kafka,
        prob_scale,
        fraud_rate,
        out_path,
        peak_hour,
        test_mode,
    ) = _resolve_run_settings(args)

    if duration <= 0 and not test_mode:
        logger.warning(
            "Running INFINITE mode (300k users possible). "
            "Use --test or --duration N for a finite run that stops automatically."
        )

    logger.info(
        "Starting generator: N=%d, M=%d, duration=%s, kafka=%s",
        n,
        m,
        "infinite" if duration <= 0 else f"{duration}s",
        use_kafka,
    )

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    ug = UserGenerator(N=n, M=m)
    users = ug.generate_users()
    ug.print_statistics()

    tg = TransactionGenerator(users)
    start_time = datetime.now(timezone.utc)

    if peak_hour:
        peak_scale = PEAK_TARGET_TPS / max(tg.expected_tps, 1e-9)
        prob_scale = max(prob_scale, peak_scale)
        logger.info(
            "Peak hour: prob_scale=%.1f (natural ~%.2f tx/s -> target ~%.0f tx/s)",
            prob_scale,
            tg.expected_tps,
            PEAK_TARGET_TPS,
        )
    elif test_mode and prob_scale == 1.0:
        prob_scale = TEST_TARGET_TPS / max(tg.expected_tps, 1e-9)
        logger.info(
            "Test mode: prob_scale=%.1f (~%.0f tx/s for %ds)",
            prob_scale,
            TEST_TARGET_TPS,
            duration,
        )
    elif args.small and prob_scale == 1.0 and tg.expected_tps < 5.0:
        prob_scale = 10.0 / max(tg.expected_tps, 1e-9)
        logger.info(
            "Small mode: auto prob_scale=%.1f (natural ~%.2f tx/s -> ~10 tx/s)",
            prob_scale,
            tg.expected_tps,
        )
    elif not test_mode and not args.small and tg.expected_tps < PEAK_TARGET_TPS:
        logger.warning(
            "Natural rate ~%.2f tx/s. Use --peak-hour for full load or --test for laptop.",
            tg.expected_tps,
        )

    kafka_producer = None
    if use_kafka:
        try:
            from .kafka_producer import KafkaTransactionProducer

            kafka_producer = KafkaTransactionProducer(
                bootstrap_servers=args.kafka_bootstrap,
                topic=args.kafka_topic,
            )
            if not kafka_producer.test_connection():
                logger.error("Kafka connection test failed — start Kafka or fix bootstrap URL")
                return 1
        except ImportError:
            logger.error("confluent-kafka not installed. Run: pip install -r requirements.txt")
            return 1
        except Exception as e:
            logger.error("Failed to initialize Kafka: %s", e)
            return 1

    collected: list = []

    def on_batch(batch, sec, _elapsed):
        if out_path and duration > 0:
            collected.extend(batch)

    try:
        summary = tg.run_continuous(
            kafka_producer=kafka_producer,
            start_time=start_time,
            duration_seconds=duration,
            real_time=real_time,
            prob_scale=prob_scale,
            fraud_rate=fraud_rate,
            verbose=True,
            should_stop=lambda: _shutdown_requested,
            on_batch=on_batch if out_path and duration > 0 else None,
        )
        logger.info("Run summary: %s", summary)
        if duration > 0:
            logger.info("Simulation complete — generator stopped.")
    finally:
        if kafka_producer is not None:
            kafka_producer.close()

    if out_path and duration > 0 and collected:
        tg.save_transactions(collected, out_path)
        logger.info("Saved %d transactions to %s", len(collected), out_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
