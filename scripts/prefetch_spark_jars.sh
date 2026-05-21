#!/usr/bin/env bash
# Download Spark Kafka connector JARs once (speeds up first test.sh --full).
set -euo pipefail

if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "spark-master not running. Run: docker compose up -d spark-master"
  exit 1
fi

echo "Prefetching Spark Kafka packages (may take 2-5 min)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.driver.memory=512m \
  /workspace/project/scripts/spark_prefetch.py

echo "Prefetch done."
