"""Order item synthetic data generator."""

from __future__ import annotations

import pandas as pd

from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import round_currency


class OrderItemGenerator(BaseGenerator):
    """Generate order line items linked to orders and products."""

    def __init__(
        self,
        config,
        orders_df: pd.DataFrame,
        products_df: pd.DataFrame,
    ) -> None:
        """
        Initialize order item generator.

        Args:
            config: Data generation configuration.
            orders_df: Generated orders for foreign keys.
            products_df: Generated products for foreign keys and pricing.
        """
        super().__init__(config)
        if orders_df.empty:
            raise ValueError("orders_df must not be empty")
        if products_df.empty:
            raise ValueError("products_df must not be empty")
        self.orders_df = orders_df
        self.products_df = products_df

    def generate(self) -> pd.DataFrame:
        """
        Generate order line items with quantity, pricing, and discounts.

        Returns:
            DataFrame aligned to ``retail.order_items`` schema.
        """
        records: list[dict[str, object]] = []
        order_item_id = 1
        active_products = self.products_df[self.products_df["is_active"]]
        if active_products.empty:
            active_products = self.products_df

        for _, order in self.orders_df.iterrows():
            num_items = int(
                self.rng.integers(
                    self.config.min_items_per_order,
                    self.config.max_items_per_order + 1,
                )
            )
            chosen_products = active_products.sample(
                n=min(num_items, len(active_products)),
                random_state=int(self.rng.integers(0, 2**31 - 1)),
            )

            for _, product in chosen_products.iterrows():
                quantity = int(self.rng.integers(1, 4))
                unit_price = float(product["unit_price"])
                discount_pct = 0.0
                if self.rng.random() < self.config.discount_probability:
                    discount_pct = float(
                        self.rng.uniform(0.05, self.config.max_discount_pct)
                    )
                line_total = round_currency(
                    unit_price * quantity * (1 - discount_pct)
                )

                records.append(
                    {
                        "order_item_id": order_item_id,
                        "order_id": int(order["order_id"]),
                        "product_id": int(product["product_id"]),
                        "quantity": quantity,
                        "unit_price": round_currency(unit_price),
                        "discount_pct": round(discount_pct, 4),
                        "line_total": line_total,
                    }
                )
                order_item_id += 1

        df = pd.DataFrame.from_records(records)
        return self._add_audit_columns(df, include_updated_at=False)
