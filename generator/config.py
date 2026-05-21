"""Shared configuration (Kafka bootstrap, topic)."""
from __future__ import annotations

import os

# apache/kafka on host: EXTERNAL listener 9094 (preferred) or PLAINTEXT 9092.
DEFAULT_KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
DEFAULT_KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
# TP5 peak-hour target throughput (transactions per second).
PEAK_TARGET_TPS = float(os.environ.get("PEAK_TARGET_TPS", "1000"))

# --test mode defaults (light load for laptops, ~1 min run)
TEST_N = int(os.environ.get("TEST_N", "75"))
TEST_M = int(os.environ.get("TEST_M", "125"))
TEST_DURATION_SEC = int(os.environ.get("TEST_DURATION_SEC", "70"))
TEST_TARGET_TPS = float(os.environ.get("TEST_TARGET_TPS", "12"))
