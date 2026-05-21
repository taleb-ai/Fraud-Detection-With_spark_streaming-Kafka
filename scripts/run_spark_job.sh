#!/usr/bin/env bash
# Submit fraud metrics Spark Structured Streaming job (run from project root).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "spark-master not running. Start: docker compose up -d"
  exit 1
fi

MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
DRIVER_MEM="${SPARK_DRIVER_MEMORY:-2g}"
EXEC_MEM="${SPARK_EXECUTOR_MEMORY:-2g}"
SHUFFLE="${SPARK_SHUFFLE_PARTITIONS:-24}"

# Laptop test: local mode inside spark-master (driver + data volume on same container)
if [ "${SPARK_LOCAL_MODE:-}" = "1" ]; then
  MASTER="local[2]"
  DRIVER_MEM="${SPARK_DRIVER_MEMORY:-1g}"
  EXEC_MEM="${SPARK_EXECUTOR_MEMORY:-1g}"
  SHUFFLE="${SPARK_SHUFFLE_PARTITIONS:-4}"
fi

echo "Submitting Spark job (master=$MASTER, deploy-mode=client)..."
DOCKER_ENV=(
  -e PYTHONPATH=/workspace/project/spark_jobs
  -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
  -e KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
  -e SPARK_CHECKPOINT_DIR=/workspace/checkpoints/fraud-metrics
  -e METRICS_DIR=/workspace/data/metrics
)
[ -n "${SPARK_TEST_MODE:-}" ] && DOCKER_ENV+=(-e "SPARK_TEST_MODE=$SPARK_TEST_MODE")
[ -n "${TRIGGER_INTERVAL:-}" ] && DOCKER_ENV+=(-e "TRIGGER_INTERVAL=$TRIGGER_INTERVAL")

docker exec "${DOCKER_ENV[@]}" spark-master /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf "spark.sql.shuffle.partitions=$SHUFFLE" \
  --conf "spark.driver.memory=$DRIVER_MEM" \
  --conf "spark.executor.memory=$EXEC_MEM" \
  /workspace/project/spark_jobs/fraud_metrics_stream.py
