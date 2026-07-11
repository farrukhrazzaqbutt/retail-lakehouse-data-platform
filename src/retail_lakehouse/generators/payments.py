"""Payment synthetic data generator."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from retail_lakehouse.config.settings import WeightedOption
from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import round_currency, weighted_choice


class PaymentGenerator(BaseGenerator):
    """Generate payment transactions linked to orders."""

    def __init__(self, config, orders_df: pd.DataFrame) -> None:
        """
        Initialize payment generator.

        Args:
            config: Data generation configuration.
            orders_df: Orders with finalized totals and statuses.
        """
        super().__init__(config)
        if orders_df.empty:
            raise ValueError("orders_df must not be empty")
        self.orders_df = orders_df

    def generate(self) -> pd.DataFrame:
        """
        Generate one payment record per order.

        Payment status is influenced by order status to preserve realism.

        Returns:
            DataFrame aligned to ``retail.payments`` schema.
        """
        records: list[dict[str, object]] = []

        for payment_id, (_, order) in enumerate(self.orders_df.iterrows(), start=1):
            payment_status = self._derive_payment_status(str(order["order_status"]))
            payment_method = weighted_choice(self.rng, self.config.payment_methods)
            amount = float(order["total_amount"])

            if payment_status == "failed":
                amount = 0.0
            elif payment_status == "refunded":
                amount = round_currency(amount * float(self.rng.uniform(0.5, 1.0)))

            payment_date = order["order_date"] + timedelta(
                minutes=int(self.rng.integers(1, 120))
            )

            records.append(
                {
                    "payment_id": payment_id,
                    "order_id": int(order["order_id"]),
                    "payment_date": payment_date,
                    "payment_method": payment_method,
                    "payment_status": payment_status,
                    "amount": round_currency(amount),
                    "currency": str(order["currency"]),
                    "transaction_ref": f"TXN-{int(order['order_id']):08d}",
                }
            )

        df = pd.DataFrame.from_records(records)
        return self._add_audit_columns(df)

    def _derive_payment_status(self, order_status: str) -> str:
        """
        Map order status to a realistic payment status.

        Failed and cancelled orders skew toward failed payments; refunded orders
        map to refunded payments; completed orders mostly succeed.
        """
        if order_status == "failed":
            return "failed"
        if order_status == "cancelled":
            return weighted_choice(
                self.rng,
                [
                    WeightedOption(name="failed", weight=0.7),
                    WeightedOption(name="pending", weight=0.3),
                ],
            )
        if order_status == "refunded":
            return "refunded"
        if order_status == "pending":
            return weighted_choice(
                self.rng,
                [
                    WeightedOption(name="pending", weight=0.8),
                    WeightedOption(name="succeeded", weight=0.2),
                ],
            )
        return weighted_choice(self.rng, self.config.payment_statuses)
