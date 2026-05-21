# Real-time Banking Fraud Detection (TP5)

**Course:** SID45 — Big Data Processing, ESP Nouakchott, 2025-2026

```
Generator  →  Kafka  →  Spark  →  data/metrics/*.parquet  →  Dashboard
```

## 1. Setup (once)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Run on your laptop (recommended)

### One command in the terminal

```bash
# First time after pulling fixes: recreate Spark containers
docker compose up -d --force-recreate spark-master spark-worker-1

./scripts/test.sh --full
```

This **automatically** starts Kafka, Spark, and Jupyter in Docker, sends fake transactions ~70s, and writes `data/metrics/*.parquet`.

Wait until you see:

```text
SUCCESS — dashboard data is ready.
```

First run may take **3–6 minutes** (Spark downloads JARs once).

### See results without Jupyter (fast)

Right after success, or anytime:

```bash
python scripts/show_metrics.py
```

Prints tables in the terminal in a few seconds. **You do not need to open the notebook** for this.

### Jupyter dashboard (§5.3 — assignment)

**Prerequisite:** `./scripts/test.sh --full` finished with **SUCCESS** and Docker is still up.

1. Open http://127.0.0.1:8888  
2. Token: `ChangeMeStrong`  
3. Open **`work/realtime_dashboard.ipynb`** (file is in `notebooks/` on your PC)  
4. Menu → **Run → Run All Cells**  

| Cell | What it does |
|------|----------------|
| 1 | Loads paths, lists `data/metrics/` files |
| 2 | Defines `render_dashboard()` |
| 3 | Shows tables + charts once |
| 4 | Auto-refresh every 5 s (stop with **Interrupt kernel**) |

The notebook shows: last 10 s of txs, last 20 users, windowed metrics (avg/count/distinct peers), lifetime stats, alerts (red), bar charts.

For a **live** refresh during a long demo, run in separate terminals:

```bash
./scripts/run_spark_job.sh          # keep running
python3 -m generator.main --test --kafka --duration 120
```

Then use notebook cell 4 while Spark is still writing new Parquet files.

## 3. Kafka only (no Spark metrics)

```bash
./scripts/test.sh
python scripts/show_metrics.py   # shows transactions_test.json fallback
```

## URLs

| URL | Service |
|-----|---------|
| http://127.0.0.1:8080 | Kafka UI |
| http://127.0.0.1:8888 | Jupyter (optional) |
| http://127.0.0.1:8081 | Spark UI |

## Cleanup

```bash
./scripts/cleanup.sh --docker
```

## Full demo (strong PC only)

```bash
docker compose --profile full up -d
bash scripts/create_kafka_topic.sh
python3 -m generator.main --kafka --peak-hour --fraud-rate 0.001
./scripts/run_spark_job.sh
```

## Main files

| Path | Role |
|------|------|
| `scripts/test.sh` | **Start here** — `--full` runs full pipeline |
| `scripts/show_metrics.py` | **View results** in terminal (no Jupyter) |
| `scripts/run_spark_job.sh` | Spark job |
| `generator/` | Fake transactions → Kafka |
| `spark_jobs/fraud_metrics_stream.py` | Spark streaming |
| `notebooks/realtime_dashboard.ipynb` | Optional visual dashboard |
| `data/metrics/` | Parquet output |

## Group members

_Add names before submission._
