#!/usr/bin/env bash
# Create or recreate the transactions topic with 6 partitions.
set -euo pipefail

TOPIC="${KAFKA_TOPIC:-transactions}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"

if ! docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
  echo "kafka container not running. Run: docker compose up -d kafka"
  exit 1
fi

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic "$TOPIC" 2>/dev/null || true

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic "$TOPIC"
