#!/usr/bin/env bash
# Safe end-to-end test for laptops (no 300k users).
#
# Usage:
#   ./scripts/test.sh              # Kafka + generator only (~70s)
#   ./scripts/test.sh --full       # Kafka + Spark + Jupyter + metrics for dashboard
#   ./scripts/cleanup.sh --docker  # free disk after a run
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FULL=0
DURATION="${TEST_DURATION_SEC:-70}"
SPARK_LOG="${ROOT}/data/spark-test.log"
SPARK_START_TIMEOUT="${SPARK_START_TIMEOUT:-300}"
METRICS_WAIT="${METRICS_WAIT_SEC:-120}"
SPARK_DRAIN_SEC="${SPARK_DRAIN_SEC:-35}"

for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    -h|--help)
      echo "Usage: $0 [--full]"
      echo "  default: starts Kafka, runs generator ${DURATION}s"
      echo "  --full:  Kafka + Spark + Jupyter; writes data/metrics/*.parquet"
      exit 0
      ;;
  esac
done

if [ -f .venv/bin/python ]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

_metrics_on_host() {
  [ -e "${ROOT}/data/metrics/recent_transactions.parquet" ] \
    || [ -e "${ROOT}/data/metrics/latest_snapshot.parquet" ]
}

_metrics_in_container() {
  docker exec spark-master test -e /workspace/data/metrics/recent_transactions.parquet 2>/dev/null \
    || docker exec spark-master test -e /workspace/data/metrics/latest_snapshot.parquet 2>/dev/null
}

_metrics_ready() {
  _metrics_on_host || _metrics_in_container
}

_prepare_spark_temp_dirs() {
  # Remove root-owned spark-local (breaks Spark uid=1000); Spark uses /tmp inside container.
  rm -rf checkpoints/spark-local 2>/dev/null || true
  if docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
    docker exec -u root spark-master bash -c \
      'rm -rf /workspace/checkpoints/spark-local; chown -R spark:spark /workspace/checkpoints; chmod -R u+rwX /workspace/checkpoints' \
      2>/dev/null || true
  fi
}

echo "=== TP5 laptop test (${DURATION}s, 200 users) ==="

if [ "$FULL" -eq 1 ]; then
  echo "Starting Kafka + Spark + Jupyter (Docker)..."
  _prepare_spark_temp_dirs
  docker compose up -d --force-recreate spark-master spark-worker-1 2>/dev/null || true
  docker compose up -d kafka kafka-ui spark-master spark-worker-1 jupyter
  _prepare_spark_temp_dirs
else
  echo "Starting Kafka only..."
  docker compose up -d kafka kafka-ui
fi

echo "Waiting 15s for Kafka..."
sleep 15
bash scripts/create_kafka_topic.sh 2>/dev/null || true

$PY -c "
from generator.kafka_producer import KafkaTransactionProducer
p = KafkaTransactionProducer()
raise SystemExit(0 if p.test_connection() else 1)
" || { echo "Kafka not ready — run: docker compose logs kafka --tail 20"; exit 1; }

if [ "$FULL" -eq 1 ]; then
  echo ""
  echo "=== Preparing metrics output ==="
  rm -rf checkpoints/fraud-metrics 2>/dev/null || true
  find data/metrics -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} + 2>/dev/null || true
  rm -f data/spark-test.log data/spark-batch.log 2>/dev/null || true
  mkdir -p data/metrics checkpoints data
  _prepare_spark_temp_dirs
  : > "$SPARK_LOG"

  echo "Prefetching Spark JARs (skip if already cached)..."
  bash scripts/prefetch_spark_jars.sh >>"$SPARK_LOG" 2>&1 || true

  echo ""
  echo "=== Starting Spark (local mode, writes data/metrics/) ==="
  export KAFKA_STARTING_OFFSETS=earliest
  export SPARK_TEST_MODE=1
  export SPARK_LOCAL_MODE=1
  export SPARK_TEST_SECONDS=$((DURATION + SPARK_START_TIMEOUT + METRICS_WAIT + 60))

  timeout "${SPARK_TEST_SECONDS}" bash scripts/run_spark_job.sh >>"$SPARK_LOG" 2>&1 &
  SPARK_PID=$!

  echo "Waiting for Spark job to start (max ${SPARK_START_TIMEOUT}s)..."
  started=0
  for ((i = 1; i <= SPARK_START_TIMEOUT / 2; i++)); do
    if grep -q "Fraud metrics streaming job started" "$SPARK_LOG" 2>/dev/null; then
      echo "Spark is running (${i}x2s)."
      started=1
      break
    fi
    if ! kill -0 "$SPARK_PID" 2>/dev/null; then
      echo "ERROR: Spark exited before starting."
      tail -40 "$SPARK_LOG" || true
      docker logs spark-master --tail 20 2>/dev/null || true
      exit 1
    fi
    sleep 2
  done
  if [ "$started" -eq 0 ]; then
    echo "WARNING: Spark slow to start — continuing (see $SPARK_LOG)"
  fi
fi

echo ""
echo "=== Generator (${DURATION}s) → Kafka ==="
$PY -m generator.main --test --kafka --duration "$DURATION"

if [ "$FULL" -eq 1 ]; then
  echo ""
  echo "Waiting ${SPARK_DRAIN_SEC}s for Spark to process Kafka backlog..."
  sleep "$SPARK_DRAIN_SEC"
  echo "Checking Parquet metrics (max ${METRICS_WAIT}s)..."
  ready=0
  for ((i = 1; i <= METRICS_WAIT / 2; i++)); do
    if _metrics_ready; then
      ready=1
      echo "Metrics ready (${i}x2s)."
      break
    fi
    sleep 2
  done

  if kill -0 "$SPARK_PID" 2>/dev/null; then
    kill "$SPARK_PID" 2>/dev/null || true
    wait "$SPARK_PID" 2>/dev/null || true
  fi

  echo ""
  echo "--- Host data/metrics ---"
  ls -la data/metrics/ 2>/dev/null || true
  echo "--- Container /workspace/data/metrics ---"
  docker exec spark-master ls -la /workspace/data/metrics/ 2>/dev/null || true

  if [ -f data/spark-batch.log ]; then
    echo "--- spark-batch.log (last 5 lines) ---"
    tail -5 data/spark-batch.log || true
  fi

  if [ "$ready" -eq 1 ]; then
    echo ""
    echo "SUCCESS — dashboard data is ready."
    echo ""
    echo ">>> Quick view (no Jupyter):"
    $PY scripts/show_metrics.py 2>/dev/null || {
      echo "  (install: pip install pandas pyarrow)"
    }
    echo ""
    echo ">>> Optional Jupyter: http://127.0.0.1:8888 (token ChangeMeStrong)"
    echo "    work/realtime_dashboard.ipynb"
  else
    echo ""
    echo "FAILED — no metrics in data/metrics/"
    echo "--- spark-test.log (last 60 lines) ---"
    tail -60 "$SPARK_LOG" || true
    echo "--- spark-master logs ---"
    docker logs spark-master --tail 30 2>/dev/null || true
    exit 1
  fi
fi

echo ""
echo "=== Done ==="
echo "Kafka UI: http://127.0.0.1:8080"
if [ "$FULL" -eq 0 ]; then
  echo "Dashboard: ./scripts/test.sh --full"
fi
