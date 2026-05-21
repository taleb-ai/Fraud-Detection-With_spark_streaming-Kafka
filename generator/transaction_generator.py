"""Transaction generation logic for the banking fraud simulator (TP5 §4.3, §5.1)."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

import numpy as np

from .message_format import to_bank_message

logger = logging.getLogger(__name__)


class TransactionGenerator:
    """Simulate transactions between users using per-second Bernoulli probabilities."""

    def __init__(self, users: List[Dict]) -> None:
        if not isinstance(users, list):
            raise ValueError("users must be a list of dicts")
        self.users = users
        self.n = len(users)

        self.user_ids = [u["user_id"] for u in users]
        self.banks = [u["bank"] for u in users]
        self.monthly_incomes = np.array([u["monthly_income"] for u in users], dtype=float)
        self.avg_spending = np.array([u["avg_spending"] for u in users], dtype=float)
        self.probs = np.array([u.get("prob_per_second", 0.0) for u in users], dtype=float)
        self.balances = np.array([u["balance"] for u in users], dtype=float)

        self.expected_tps = float(self.probs.sum())
        logger.info(
            "TransactionGenerator ready: %d users, expected ~%.2f tx/s from probabilities",
            self.n,
            self.expected_tps,
        )

    def _sync_users(self) -> None:
        for idx, u in enumerate(self.users):
            u["balance"] = float(self.balances[idx])

    def _sample_amount(self, sender_idx: int) -> float:
        """Amount A ~ Uniform[Si - 2*sigma, Si + 2*sigma], sigma = Si/2 (spec §4.3)."""
        si = float(self.avg_spending[sender_idx])
        sigma = max(0.01, si / 2.0)
        low = max(0.01, si - 2.0 * sigma)
        high = si + 2.0 * sigma
        return round(float(np.random.uniform(low, high)), 2)

    def _pick_receiver(self, sender_idx: int) -> int:
        receiver_idx = sender_idx
        while receiver_idx == sender_idx:
            receiver_idx = int(np.random.randint(0, self.n))
        return receiver_idx

    def generate_second_batch(self, current_time: Optional[datetime] = None) -> List[Dict]:
        """Generate all transactions for one simulated second (vectorized Bernoulli)."""
        if self.n < 2:
            return []

        ts = current_time or datetime.now(timezone.utc)
        active_mask = np.random.random(self.n) < self.probs
        sender_indices = np.flatnonzero(active_mask)
        if sender_indices.size == 0:
            return []

        txs: List[Dict] = []
        for sender_idx in sender_indices:
            sender_idx = int(sender_idx)
            amount = self._sample_amount(sender_idx)
            if amount > float(self.balances[sender_idx]):
                continue

            receiver_idx = self._pick_receiver(sender_idx)
            self.balances[sender_idx] -= amount
            self.balances[receiver_idx] += amount

            txs.append(
                to_bank_message(
                    sender_id=self.user_ids[sender_idx],
                    sender_bank=self.banks[sender_idx],
                    receiver_id=self.user_ids[receiver_idx],
                    receiver_bank=self.banks[receiver_idx],
                    amount=amount,
                    timestamp=ts,
                )
            )

        if txs:
            self._sync_users()
        return txs

    def add_monthly_deposits(self) -> None:
        self.balances += self.monthly_incomes
        self._sync_users()
        logger.info("Added monthly deposits to %d users", self.n)

    def _maybe_monthly_deposit(self, current_time: datetime) -> None:
        if (
            current_time.day == 1
            and current_time.hour == 0
            and current_time.minute == 0
            and current_time.second == 0
        ):
            self.add_monthly_deposits()

    def run_continuous(
        self,
        *,
        kafka_producer=None,
        start_time: Optional[datetime] = None,
        duration_seconds: int = 0,
        real_time: bool = True,
        prob_scale: float = 1.0,
        fraud_rate: float = 0.0,
        verbose: bool = True,
        should_stop: Optional[Callable[[], bool]] = None,
        on_batch: Optional[Callable[[List[Dict], int, float], None]] = None,
    ) -> Dict:
        """Run simulation loop (infinite if duration_seconds <= 0).

        Returns summary stats dict.
        """
        original_probs = None
        if prob_scale != 1.0:
            original_probs = self.probs.copy()
            self.probs = np.minimum(original_probs * prob_scale, 1.0)
            self.expected_tps = float(self.probs.sum())
            logger.info("Applied prob_scale=%.2f -> expected ~%.1f tx/s", prob_scale, self.expected_tps)

        current_time = start_time or datetime.now(timezone.utc)
        sec = 0
        total_txs = 0
        total_sent_kafka = 0
        loop_start = time.monotonic()
        last_log = loop_start

        stop = should_stop or (lambda: False)
        mode = "infinite" if duration_seconds <= 0 else f"{duration_seconds}s"
        logger.info(
            "Starting continuous simulation (%s, real_time=%s, kafka=%s)",
            mode,
            real_time,
            kafka_producer is not None,
        )

        try:
            while not stop():
                if duration_seconds > 0 and sec >= duration_seconds:
                    break

                tick_start = time.monotonic()
                self._maybe_monthly_deposit(current_time)
                batch = self.generate_second_batch(current_time=current_time)
                if fraud_rate > 0:
                    from .fraud_injection import inject_fraud

                    batch = inject_fraud(
                        batch, self.users, fraud_rate=fraud_rate, current_time=current_time
                    )

                if kafka_producer is not None and batch:
                    sent = kafka_producer.send_batch(batch)
                    total_sent_kafka += sent
                    if sent != len(batch):
                        logger.warning("Kafka delivery incomplete: %d/%d", sent, len(batch))

                total_txs += len(batch)
                if on_batch is not None:
                    on_batch(batch, sec, time.monotonic() - loop_start)

                now = time.monotonic()
                if verbose and (now - last_log >= 10.0 or sec < 3):
                    elapsed = now - loop_start
                    tps = total_txs / elapsed if elapsed > 0 else 0.0
                    logger.info(
                        "sec=%d batch=%d cumulative=%d kafka_sent=%d avg_tps=%.1f",
                        sec + 1,
                        len(batch),
                        total_txs,
                        total_sent_kafka,
                        tps,
                    )
                    last_log = now

                sec += 1
                current_time += timedelta(seconds=1)

                if real_time:
                    elapsed_tick = time.monotonic() - tick_start
                    sleep_for = max(0.0, 1.0 - elapsed_tick)
                    if sleep_for > 0:
                        time.sleep(sleep_for)
        finally:
            if original_probs is not None:
                self.probs = original_probs
                self.expected_tps = float(self.probs.sum())

        elapsed = time.monotonic() - loop_start
        summary = {
            "seconds_simulated": sec,
            "total_transactions": total_txs,
            "total_sent_kafka": total_sent_kafka,
            "elapsed_wall_seconds": elapsed,
            "average_tps": total_txs / elapsed if elapsed > 0 else 0.0,
        }
        logger.info("Simulation stopped: %s", summary)
        return summary

    def save_transactions(self, transactions: List[Dict], filename: str) -> None:
        import os

        os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(transactions, f, indent=2)
        logger.info("Saved %d transactions to %s", len(transactions), filename)
