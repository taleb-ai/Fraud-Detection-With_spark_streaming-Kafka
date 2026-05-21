"""Bank transaction message format (TP5 §2.2)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict

BANK_ENTITIES = {
    "X": "bank_X",
    "A": "bank_A",
    "B": "bank_B",
}


def bank_code_to_entity(code: str) -> str:
    """Map internal bank code ('X', 'A', 'B') to spec entity name."""
    return BANK_ENTITIES.get(code, f"bank_{code}")


def format_timestamp(dt: datetime) -> str:
    """ISO 8601 UTC timestamp with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_bank_message(
    *,
    sender_id: str,
    sender_bank: str,
    receiver_id: str,
    receiver_bank: str,
    amount: float,
    timestamp: datetime,
    tx_id: str | None = None,
    msg_entity: str = "bank_X",
    app_type: str = "mobile_app",
    tx_type: str = "transfer",
) -> Dict:
    """Build a transaction dict matching the bank JSON specification."""
    return {
        "msg_entity": msg_entity,
        "app_type": app_type,
        "send_entity": bank_code_to_entity(sender_bank),
        "receive_entity": bank_code_to_entity(receiver_bank),
        "send_id": sender_id,
        "receive_id": receiver_id,
        "amount": round(float(amount), 2),
        "date": format_timestamp(timestamp),
        "tx_type": tx_type,
        "tx_id": tx_id or str(uuid.uuid4()),
    }
