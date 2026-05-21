#!/usr/bin/env python3
"""Print dashboard summary in the terminal (seconds; no Jupyter)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS_DIR = Path(os.environ.get("METRICS_DIR", ROOT / "data" / "metrics"))
JSON_FALLBACK = ROOT / "data" / "transactions_test.json"


def _exists(path: Path) -> bool:
    return path.exists()


def load_parquet(path: Path):
    import pandas as pd

    if not _exists(path):
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"  (could not read {path}: {e})")
        return None


def main() -> int:
    print(f"=== Metrics summary ===\nMetrics dir: {METRICS_DIR}")
    if METRICS_DIR.is_dir():
        print("Files:", sorted(p.name for p in METRICS_DIR.iterdir()))
    else:
        print("Files: (directory missing)")

    recent_path = METRICS_DIR / "recent_transactions.parquet"
    snapshot_path = METRICS_DIR / "latest_snapshot.parquet"
    alerts_path = METRICS_DIR / "alerts.parquet"

    ready = _exists(recent_path) or _exists(snapshot_path)
    print(f"\nSpark metrics ready: {ready}")

    if ready:
        recent = load_parquet(recent_path)
        if recent is not None and not recent.empty:
            print(f"\n## Recent transactions ({len(recent)} rows, last 10)")
            cols = [c for c in ["send_id", "receive_id", "amount", "date", "sim_fraud"] if c in recent.columns]
            print(recent[cols].tail(10).to_string(index=False))

        metrics = load_parquet(snapshot_path)
        if metrics is not None and not metrics.empty:
            print(f"\n## Top users by tx_count ({len(metrics)} metric rows)")
            show = [c for c in [
                "user_id", "direction", "window_name", "tx_count", "avg_amount", "distinct_peers"
            ] if c in metrics.columns]
            if "user_id" in metrics.columns:
                metrics = metrics[~metrics["user_id"].astype(str).str.startswith("__")]
            top = metrics.sort_values("tx_count", ascending=False).head(15)
            print(top[show].to_string(index=False))
            if "window_name" in metrics.columns:
                print("\n## Volume by window")
                print(metrics.groupby("window_name")["tx_count"].sum().sort_values(ascending=False).to_string())

        alerts = load_parquet(alerts_path)
        if alerts is not None and not alerts.empty:
            print(f"\n## Alerts ({len(alerts)} rows)")
            print(alerts.head(10).to_string(index=False))
        else:
            print("\n## Alerts: none")
        return 0

    if JSON_FALLBACK.is_file():
        print(f"\n(Fallback) {JSON_FALLBACK.name}")
        with open(JSON_FALLBACK, encoding="utf-8") as f:
            txs = json.load(f)
        print(f"Transactions: {len(txs)}")
        if txs:
            print("\nLast 5:")
            for t in txs[-5:]:
                print(f"  {t.get('send_id')} -> {t.get('receive_id')}  {t.get('amount')}  {t.get('date')}")
        print("\nRun ./scripts/test.sh --full for Spark Parquet metrics.")
        return 0

    print("\nNo metrics found.")
    print("  ./scripts/test.sh --full   # creates data/metrics/*.parquet")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ImportError:
        print("Install: pip install pandas pyarrow")
        sys.exit(1)
