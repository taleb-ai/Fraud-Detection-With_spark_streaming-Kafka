"""Inject simulated fraud patterns into transaction batches (TP5 §4.4 demo)."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np

from .message_format import to_bank_message


def inject_fraud(
    batch: List[Dict],
    users: List[Dict],
    fraud_rate: float = 0.001,
    current_time: Optional[datetime] = None,
) -> List[Dict]:
    """Append fraud-pattern transactions to a second batch.

    Patterns:
    - velocity burst: one sender, many txs
    - large amount: amount >> avg_spending
    - fan-out: one sender, many distinct receivers
    """
    if fraud_rate <= 0 or not users or len(users) < 2:
        return batch

    ts = current_time or datetime.utcnow()
    n_fraud = max(1, int(np.random.poisson(lam=len(batch) * fraud_rate)))
    fraud_txs: List[Dict] = []

    for _ in range(n_fraud):
        sender = users[int(np.random.randint(0, len(users)))]
        pattern = int(np.random.choice(3))

        if pattern == 0:
            # velocity burst (same sender, moderate amounts)
            receiver = users[int(np.random.randint(0, len(users)))]
            if receiver["user_id"] == sender["user_id"]:
                continue
            amount = float(round(sender["avg_spending"] * 2, 2))
            fraud_txs.append(
                _fraud_msg(sender, receiver, amount, ts, pattern_name="velocity_burst")
            )
        elif pattern == 1:
            # large amount
            receiver = users[int(np.random.randint(0, len(users)))]
            if receiver["user_id"] == sender["user_id"]:
                continue
            amount = float(round(sender["avg_spending"] * 15, 2))
            fraud_txs.append(
                _fraud_msg(sender, receiver, amount, ts, pattern_name="large_amount")
            )
        else:
            # fan-out: one sender to random receiver
            receiver = users[int(np.random.randint(0, len(users)))]
            if receiver["user_id"] == sender["user_id"]:
                continue
            amount = float(round(sender["avg_spending"] * 3, 2))
            fraud_txs.append(
                _fraud_msg(sender, receiver, amount, ts, pattern_name="fan_out")
            )

    return batch + fraud_txs


def _fraud_msg(sender: Dict, receiver: Dict, amount: float, ts: datetime, pattern_name: str) -> Dict:
    msg = to_bank_message(
        sender_id=sender["user_id"],
        sender_bank=sender["bank"],
        receiver_id=receiver["user_id"],
        receiver_bank=receiver["bank"],
        amount=amount,
        timestamp=ts,
        tx_id=str(uuid.uuid4()),
    )
    msg["sim_fraud"] = True
    msg["fraud_pattern"] = pattern_name
    return msg
