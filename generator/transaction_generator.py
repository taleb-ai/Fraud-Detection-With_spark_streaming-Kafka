"""Transaction generation logic for the banking fraud simulator.

Provides `TransactionGenerator` which produces transactions per-second
according to user probabilities and updates balances.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class TransactionGenerator:
    """Simulate transactions between users.

    Parameters
    ----------
    users: list of dict
        Users produced by `UserGenerator.generate_users()`
    """

    def __init__(self, users: List[Dict]) -> None:
        if not isinstance(users, list):
            raise ValueError("users must be a list of dicts")
        self.users = users
        self.n = len(users)

        # Create fast-access arrays
        self.user_ids = [u["user_id"] for u in users]
        self.banks = [u["bank"] for u in users]
        self.monthly_incomes = np.array([u["monthly_income"] for u in users], dtype=float)
        self.avg_spending = np.array([u["avg_spending"] for u in users], dtype=float)
        self.probs = np.array([u.get("prob_per_second", 0.0) for u in users], dtype=float)
        self.balances = np.array([u["balance"] for u in users], dtype=float)

        # Map id -> index
        self.id_to_index = {uid: idx for idx, uid in enumerate(self.user_ids)}

    def _sync_users(self) -> None:
        """Write back balances to the user dicts."""
        for idx, u in enumerate(self.users):
            u["balance"] = float(self.balances[idx])

    def generate_single_transaction(self, current_time: Optional[datetime] = None) -> Optional[Dict]:
        """Generate a single transaction or return None if not possible.

        The sender is chosen weighted by `prob_per_second`. The receiver is
        chosen uniformly at random among other users.
        """
        if self.n < 2:
            return None

        p_sum = float(self.probs.sum())
        if p_sum <= 0.0:
            return None

        # choose sender index
        sender_idx = int(np.random.choice(self.n, p=(self.probs / p_sum)))

        # choose receiver index uniformly (different from sender)
        receiver_idx = sender_idx
        while receiver_idx == sender_idx:
            receiver_idx = int(np.random.randint(0, self.n))

        avg = float(self.avg_spending[sender_idx])
        sigma = max(0.01, avg / 2.0)
        amount = float(np.random.normal(loc=avg, scale=sigma))
        amount = max(1.0, amount)
        amount = float(round(amount, 2))

        if amount > float(self.balances[sender_idx]):
            # insufficient funds
            return None

        # perform transfer
        self.balances[sender_idx] -= amount
        self.balances[receiver_idx] += amount
        self._sync_users()

        tx = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": (current_time or datetime.utcnow()).isoformat() + "Z",
            "sender_id": self.user_ids[sender_idx],
            "sender_bank": self.banks[sender_idx],
            "receiver_id": self.user_ids[receiver_idx],
            "receiver_bank": self.banks[receiver_idx],
            "amount": float(amount),
            "sender_balance_after": float(round(self.balances[sender_idx], 2)),
            "receiver_balance_after": float(round(self.balances[receiver_idx], 2)),
        }
        return tx

    def generate_batch(self, target_tps: float = 1000.0, current_time: Optional[datetime] = None) -> List[Dict]:
        """Generate transactions for a single second.

        Parameters
        ----------
        target_tps: float
            Expected transactions per second (Poisson mean)

        Returns
        -------
        list of transaction dicts
        """
        if target_tps <= 0:
            return []

        num = int(np.random.poisson(lam=float(target_tps)))
        txs: List[Dict] = []
        attempts = 0
        max_attempts = max(1000, num * 10)
        while len(txs) < num and attempts < max_attempts:
            attempts += 1
            tx = self.generate_single_transaction(current_time=current_time)
            if tx is not None:
                txs.append(tx)

        if attempts >= max_attempts and len(txs) < num:
            logger.debug("Stopped early: produced %d/%d transactions", len(txs), num)

        return txs

    def add_monthly_deposits(self) -> None:
        """Add each user's monthly_income to their balance (monthly deposit)."""
        self.balances += self.monthly_incomes
        self._sync_users()
        logger.info("Added monthly deposits to %d users", self.n)

    def run_simulation(self, duration_seconds: int = 10, target_tps: float = 10.0,
                       verbose: bool = True, start_time: Optional[datetime] = None) -> List[Dict]:
        """Run the simulator for `duration_seconds` seconds.

        Parameters
        ----------
        duration_seconds: int
            Number of seconds to simulate.
        target_tps: float
            Expected transactions per second.
        verbose: bool
            If True, logs progress.
        start_time: datetime or None
            Starting timestamp for generated transactions. Defaults to UTC now.
        """
        if duration_seconds <= 0:
            return []

        current_time = start_time or datetime.utcnow()
        txs: List[Dict] = []

        for sec in range(duration_seconds):
            # check monthly deposit: if current_time is 1st day at midnight
            if current_time.day == 1 and current_time.hour == 0 and current_time.minute == 0 and current_time.second == 0:
                self.add_monthly_deposits()

            batch = self.generate_batch(target_tps=target_tps, current_time=current_time)
            txs.extend(batch)

            if verbose and (sec % max(1, duration_seconds // 10) == 0):
                logger.info("Simulated second %d/%d — generated %d txs (cumulative %d)",
                            sec + 1, duration_seconds, len(batch), len(txs))

            current_time += timedelta(seconds=1)

        logger.info("Simulation complete: produced %d transactions", len(txs))
        return txs

    def save_transactions(self, transactions: List[Dict], filename: str) -> None:
        """Save transactions to a JSON file.

        Parameters
        ----------
        transactions: list of dict
            Transaction dicts as produced by the generator.
        filename: str
            Destination file path.
        """
        try:
            import os
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(transactions, f, indent=2)
            logger.info("Saved %d transactions to %s", len(transactions), filename)
        except Exception:
            logger.exception("Failed to save transactions to %s", filename)
            raise

    def run_simulation_with_kafka(self, duration_seconds: int = 10, target_tps: float = 10,
                                   kafka_producer=None, verbose: bool = True,
                                   start_time: Optional[datetime] = None) -> List[Dict]:
        """Run the simulator and send transactions to Kafka in real-time.

        This method combines simulation with live Kafka streaming. If no Kafka
        producer is provided, falls back to regular simulation.

        Parameters
        ----------
        duration_seconds: int
            Number of seconds to simulate.
        target_tps: float
            Expected transactions per second.
        kafka_producer: KafkaTransactionProducer or None
            Kafka producer instance. If None, uses run_simulation() as fallback.
        verbose: bool
            If True, logs progress.
        start_time: datetime or None
            Starting timestamp for generated transactions. Defaults to UTC now.

        Returns
        -------
        list of dict
            All transactions generated during simulation.
        """
        if kafka_producer is None:
            logger.warning("No Kafka producer provided, falling back to regular simulation")
            return self.run_simulation(duration_seconds=duration_seconds, target_tps=target_tps,
                                      verbose=verbose, start_time=start_time)

        if duration_seconds <= 0:
            return []

        current_time = start_time or datetime.utcnow()
        txs: List[Dict] = []

        logger.info("Starting simulation with Kafka streaming (duration=%ds, target_tps=%.1f)",
                    duration_seconds, target_tps)

        for sec in range(duration_seconds):
            # check monthly deposit: if current_time is 1st day at midnight
            if current_time.day == 1 and current_time.hour == 0 and current_time.minute == 0 and current_time.second == 0:
                self.add_monthly_deposits()

            batch = self.generate_batch(target_tps=target_tps, current_time=current_time)

            # Send to Kafka
            if batch:
                try:
                    count = kafka_producer.send_batch(batch)
                    if verbose:
                        logger.info("Sent %d/%d transactions to Kafka", count, len(batch))
                except Exception as e:
                    logger.error("Error sending batch to Kafka: %s", e)

            txs.extend(batch)

            if verbose and (sec % max(1, duration_seconds // 10) == 0):
                logger.info("Simulated second %d/%d — generated %d txs (cumulative %d)",
                            sec + 1, duration_seconds, len(batch), len(txs))

            current_time += timedelta(seconds=1)

        logger.info("Simulation with Kafka complete: produced and sent %d transactions", len(txs))
        return txs
