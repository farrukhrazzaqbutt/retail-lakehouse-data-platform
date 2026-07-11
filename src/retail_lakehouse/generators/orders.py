"""Order synthetic data generator."""

from __future__ import annotations

from datetime import UTC, datetime, time

import pandas as pd

from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import round_currency, weighted_choice


class OrderGenerator(BaseGenerator):
    """Generate order header records linked to customers."""

    def __init__(self, config, customers_df: pd.DataFrame) -> None:
        """
        Initialize order generator.

        Args:
            config: Data generation configuration.
            customers_df: Generated customers used for foreign keys and geography.
        """
        super().__init__(config)
        if customers_df.empty:
            raise ValueError("customers_df must not be empty")
        self.customers_df = customers_df

    def generate(self) -> pd.DataFrame:
        """
        Generate order header records.

        Returns:
            DataFrame aligned to ``retail.orders`` schema with placeholder amounts.
            Amounts are finalized after order items are generated.
        """
        count = self.config.num_orders
        records: list[dict[str, object]] = []

        for order_id in range(1, count + 1):
            customer = self.customers_df.iloc[
                int(self.rng.integers(0, len(self.customers_df)))
            ]
            order_status = weighted_choice(self.rng, self.config.order_statuses)
            order_date = self._random_order_timestamp()

            records.append(
                {
                    "order_id": order_id,
                    "customer_id": int(customer["customer_id"]),
                    "order_date": order_date,
                    "order_status": order_status,
                    "shipping_country": customer["country_code"],
                    "shipping_city": customer["city"],
                    "currency": "USD",
                    "subtotal_amount": 0.0,
                    "discount_amount": 0.0,
                    "shipping_amount": round_currency(float(self.rng.uniform(0, 15))),
                    "tax_amount": 0.0,
                    "total_amount": 0.0,
                }
            )

        df = pd.DataFrame.from_records(records)
        return self._add_audit_columns(df)

    def _random_order_timestamp(self) -> datetime:
        """Generate an order timestamp within the configured date range."""
        order_day = self.faker.date_between(
            start_date=self.config.order_start_date,
            end_date=self.config.order_end_date,
        )
        hour = int(self.rng.integers(0, 24))
        minute = int(self.rng.integers(0, 60))
        second = int(self.rng.integers(0, 60))
        return datetime.combine(order_day, time(hour, minute, second), tzinfo=UTC)

    @staticmethod
    def finalize_amounts(
        orders_df: pd.DataFrame, order_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Compute order-level monetary fields from order line items.

        Args:
            orders_df: Order headers with placeholder amounts.
            order_items_df: Generated order line items.

        Returns:
            Orders DataFrame with subtotal, discount, tax, and total populated.
        """
        if orders_df.empty:
            return orders_df

        items = order_items_df.copy()
        items["gross_line"] = items["unit_price"] * items["quantity"]
        line_totals = items.groupby("order_id", as_index=False).agg(
            subtotal_amount=("line_total", "sum"),
            gross_amount=("gross_line", "sum"),
        )

        amount_columns = [
            "subtotal_amount",
            "discount_amount",
            "tax_amount",
            "total_amount",
        ]
        result = (
            orders_df.drop(columns=amount_columns, errors="ignore")
            .merge(
                line_totals,
                on="order_id",
                how="left",
            )
            .fillna({"subtotal_amount": 0.0, "gross_amount": 0.0})
        )
        result["discount_amount"] = (
            result["gross_amount"] - result["subtotal_amount"]
        ).clip(lower=0)
        result["tax_amount"] = (result["subtotal_amount"] * 0.08).apply(round_currency)
        result["total_amount"] = (
            result["subtotal_amount"] + result["shipping_amount"] + result["tax_amount"]
        ).apply(round_currency)
        result["subtotal_amount"] = result["subtotal_amount"].apply(round_currency)
        result["discount_amount"] = result["discount_amount"].apply(round_currency)
        return result.drop(columns=["gross_amount"])
