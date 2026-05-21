#!/usr/bin/env bash
# Free disk space used by this project: local data, checkpoints, Docker volumes, Spark temp.
#
# Usage:
#   ./scripts/cleanup.sh          # safe default (project only)
#   ./scripts/cleanup.sh --docker # also stop compose and remove fraud-detection volumes
#   ./scripts/cleanup.sh --all    # + prune unused Docker build cache (asks confirm)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DOCKER=0
PRUNE=0
for arg in "$@"; do
  case "$arg" in
    --docker) DOCKER=1 ;;
    --all) DOCKER=1; PRUNE=1 ;;
    -h|--help)
      echo "Usage: $0 [--docker] [--all]"
      exit 0
      ;;
  esac
done

echo "=== Cleaning project files in $ROOT ==="

# Generated transaction JSON (can be large after full runs)
rm -f data/transactions.json data/transactions_test.json
find data/metrics -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} + 2>/dev/null || true
find data -name '*.parquet' -delete 2>/dev/null || true

# Spark / streaming checkpoints
rm -rf checkpoints/fraud-metrics checkpoints/spark-local checkpoints/* 2>/dev/null || true
mkdir -p checkpoints data/metrics
touch checkpoints/.gitkeep data/metrics/.gitkeep 2>/dev/null || true

# Python cache
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
find . -name '*.pyc' -delete 2>/dev/null || true

echo "Project folders cleaned."

if [ "$DOCKER" -eq 1 ]; then
  echo ""
  echo "=== Stopping fraud-detection Docker stack ==="
  docker compose down --remove-orphans 2>/dev/null || true

  echo "=== Removing fraud-detection named volumes (Kafka, HDFS, Postgres) ==="
  for v in fraud-detection_kafka_data fraud-detection_metastore_db \
           fraud-detection_hdfs_namenode fraud-detection_hdfs_datanode \
           fraud-detection_hadoop_tmp; do
    docker volume rm "$v" 2>/dev/null && echo "  removed $v" || true
  done

  echo "=== Stopping optional HDFS/Hive (heavy, often not needed) ==="
  docker rm -f hadoop-hive-single hive-metastore-db 2>/dev/null && echo "  removed HDFS/Hive containers" || true
  for v in fraud-detection_metastore_db fraud-detection_hdfs_namenode \
           fraud-detection_hdfs_datanode fraud-detection_hadoop_tmp; do
    docker volume rm "$v" 2>/dev/null && echo "  removed volume $v" || true
  done

  echo "=== Recreating Spark worker containers (clears ~2GB temp layers each) ==="
  for c in spark-worker-1 spark-worker-2 spark-worker-3 spark-master jupyter kafka kafka-ui; do
    docker rm -f "$c" 2>/dev/null && echo "  removed container $c" || true
  done
fi

if [ "$PRUNE" -eq 1 ]; then
  echo ""
  echo "=== Docker system prune (unused images, stopped containers, build cache) ==="
  docker system df
  read -r -p "Run 'docker system prune -af --volumes'? This affects ALL projects [y/N] " ans
  if [ "$ans" = "y" ] || [ "$ans" = "Y" ]; then
    docker system prune -af --volumes
  fi
fi

echo ""
echo "=== Disk usage now ==="
du -sh "$ROOT"/* 2>/dev/null | sort -hr | head -10
docker system df 2>/dev/null || true
echo ""
echo "Done. For light tests use: python3 -m generator.main --test --kafka"
echo "Start minimal Docker: docker compose up -d"
