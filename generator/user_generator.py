"""User profile generator for banking fraud simulation.

Provides `UserGenerator` which creates realistic user profiles using a
power-law income distribution and other attributes required by the
transaction simulator.

Usage example::

    gen = UserGenerator(N=100, M=200)
    users = gen.generate_users()
    gen.save_to_file('data/users.json')

"""
from __future__ import annotations

import json
import logging
import math
from typing import List, Dict

import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class UserGenerator:
    """Generate user profiles for the transaction simulator.

    Attributes generated for each user:
    - user_id: string "U_XXXXXX"
    - bank: 'X', 'A' or 'B'
    - monthly_income: float
    - avg_spending: float
    - freq_per_month: float
    - prob_per_second: float
    - balance: float

    Parameters
    ----------
    N: int
        Number of users for bank X (default 100000)
    M: int
        Number of external users for banks A and B (default 200000)
    """

    def __init__(self, N: int = 100000, M: int = 200000) -> None:
        self.N = int(N)
        self.M = int(M)
        self.users: List[Dict] = []

    def _power_law_income(self, size: int, min_income: float = 1000.0,
                          max_income: float = 100000.0,
                          alpha: float = 2.0) -> np.ndarray:
        """Sample incomes from a power-law PDF P(I) ~ 1 / I**alpha.

        Uses inverse transform sampling to produce values in [min_income, max_income].

        Parameters
        ----------
        size: int
            Number of samples to draw.
        min_income, max_income: float
            Lower and upper bounds for incomes.
        alpha: float
            Power-law exponent (alpha > 1 recommended).

        Returns
        -------
        np.ndarray
            Array of sampled incomes (float)
        """
        if size <= 0:
            return np.array([])

        if not (min_income > 0 and max_income > min_income):
            raise ValueError("Invalid income bounds")

        # Inverse-CDF sampling for power-law: for alpha != 1
        r = np.random.random(size=size)
        a = 1.0 - alpha
        xmin_a = min_income ** a
        xmax_a = max_income ** a
        sampled = (xmin_a + (xmax_a - xmin_a) * r) ** (1.0 / a)
        return sampled.astype(float)

    def generate_users(self) -> List[Dict]:
        """Create and return the user list according to the spec.

        Returns
        -------
        list of dict
            Each dict contains user attributes used by the simulator.
        """
        total = self.N + self.M
        logger.info("Generating %d users (%d X, %d external)", self.N, self.M)

        # Sample incomes
        incomes = self._power_law_income(total)

        users: List[Dict] = []
        # banks assignment: first N -> X, next M split between A and B
        for i in range(total):
            user_id = f"U_{i:06d}"
            bank = "X" if i < self.N else ("A" if (i - self.N) % 2 == 0 else "B")
            monthly_income = float(incomes[i])

            low = max(monthly_income / 1000.0, 0.01)
            high = max(monthly_income / 100.0, low + 0.01)
            avg_spending = float(np.random.uniform(low, high))

            freq_per_month = monthly_income / avg_spending if avg_spending > 0 else 0.0
            seconds_per_month = 30 * 24 * 3600
            prob_per_second = freq_per_month / seconds_per_month

            balance = float(np.random.uniform(0.0, 3.0 * monthly_income))

            user = {
                "user_id": user_id,
                "bank": bank,
                "monthly_income": float(monthly_income),
                "avg_spending": float(avg_spending),
                "freq_per_month": float(freq_per_month),
                "prob_per_second": float(prob_per_second),
                "balance": float(balance),
            }
            users.append(user)

        self.users = users
        logger.info("User generation complete")
        return users

    def save_to_file(self, filename: str) -> None:
        """Save generated users to a JSON file.

        Parameters
        ----------
        filename: str
            Path where users will be stored as JSON list.
        """
        if not self.users:
            raise RuntimeError("No users to save. Call generate_users() first.")

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.users, f, indent=2)
            logger.info("Saved %d users to %s", len(self.users), filename)
        except Exception:
            logger.exception("Failed to save users to %s", filename)
            raise

    def load_from_file(self, filename: str) -> List[Dict]:
        """Load users from a JSON file and return them.

        Parameters
        ----------
        filename: str
            Path to JSON file created by `save_to_file`.
        """
        try:
            with open(filename, "r", encoding="utf-8") as f:
                users = json.load(f)
            # Basic validation
            if not isinstance(users, list):
                raise ValueError("User file does not contain a list")
            self.users = users
            logger.info("Loaded %d users from %s", len(users), filename)
            return users
        except Exception:
            logger.exception("Failed to load users from %s", filename)
            raise

    def print_statistics(self) -> None:
        """Print simple statistics about the generated population."""
        if not self.users:
            logger.warning("No users to summarize")
            return

        incomes = np.array([u["monthly_income"] for u in self.users], dtype=float)
        balances = np.array([u["balance"] for u in self.users], dtype=float)

        logger.info("Users: %d", len(self.users))
        logger.info("Income mean=%.2f median=%.2f min=%.2f max=%.2f",
                    float(np.mean(incomes)), float(np.median(incomes)),
                    float(np.min(incomes)), float(np.max(incomes)))
        logger.info("Balance mean=%.2f median=%.2f min=%.2f max=%.2f",
                    float(np.mean(balances)), float(np.median(balances)),
                    float(np.min(balances)), float(np.max(balances)))
