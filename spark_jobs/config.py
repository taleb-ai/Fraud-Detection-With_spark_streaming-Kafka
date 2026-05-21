"""Spark streaming job configuration (TP5 §5.2)."""
from __future__ import annotations

import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")

CHECKPOINT_DIR = os.environ.get(
    "SPARK_CHECKPOINT_DIR", "/workspace/checkpoints/fraud-metrics"
)
METRICS_DIR = os.environ.get("METRICS_DIR", "/workspace/data/metrics")
LATEST_SNAPSHOT_PATH = os.path.join(METRICS_DIR, "latest_snapshot.parquet")
LIFETIME_PATH = os.path.join(METRICS_DIR, "lifetime")
ALERTS_PATH = os.path.join(METRICS_DIR, "alerts.parquet")
RAW_RECENT_PATH = os.path.join(METRICS_DIR, "recent_transactions.parquet")

WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 minutes")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000"))
def _env_nonempty(key: str, default: str) -> str:
    val = os.environ.get(key, default)
    return val if val and val.strip() else default


TRIGGER_INTERVAL = _env_nonempty("TRIGGER_INTERVAL", "10 seconds")

# Light test profile (set SPARK_TEST_MODE=1 via scripts/test.sh --full)
if os.environ.get("SPARK_TEST_MODE", "").lower() in ("1", "true", "yes"):
    MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "2000"))
    TRIGGER_INTERVAL = _env_nonempty("TRIGGER_INTERVAL", "10 seconds")

# (name, window duration, slide duration)
WINDOW_SPECS = [
    ("3_hours", "3 hours", "1 minute"),
    ("7_days", "7 days", "1 hour"),
    ("3_weeks", "21 days", "1 day"),
    ("3_months", "90 days", "1 day"),
]

# Alert thresholds
ALERT_TX_COUNT_3H = int(os.environ.get("ALERT_TX_COUNT_3H", "500"))
ALERT_AMOUNT_MULTIPLIER = float(os.environ.get("ALERT_AMOUNT_MULTIPLIER", "5.0"))

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
