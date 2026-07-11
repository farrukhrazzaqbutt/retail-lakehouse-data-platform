"""Product update feed generator (CSV file source for Phase 2)."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from retail_lakehouse.config.settings import FileSourcesConfig
from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import round_currency, weighted_choice


class ProductUpdateGenerator(BaseGenerator):
    """Generate product catalog update records delivered as CSV files."""

    def __init__(self, config: FileSourcesConfig, products_df: pd.DataFrame) -> None:
        """
        Initialize product update generator.

        Args:
            config: File source configuration.
            products_df: Product catalog used as the update baseline.
        """
        super().__init__(config.data_generation)
        self.file_config = config
        self.products_df = products_df
        if products_df.empty:
            raise ValueError("products_df must not be empty")

    def generate(self, num_records: int | None = None) -> pd.DataFrame:
        """
        Generate product update records.

        Args:
            num_records: Optional override for number of update rows.

        Returns:
            DataFrame of product update events.
        """
        count = num_records or self.file_config.product_updates_records
        update_types = self.file_config.product_update_types
        records: list[dict[str, object]] = []
        base_time = self.audit_timestamp()

        for update_id in range(1, count + 1):
            product = self.products_df.iloc[
                int(self.rng.integers(0, len(self.products_df)))
            ]
            update_type = weighted_choice(self.rng, update_types)
            effective_at = base_time + timedelta(hours=int(self.rng.integers(0, 72)))

            record: dict[str, object] = {
                "update_id": update_id,
                "product_id": int(product["product_id"]),
                "sku": str(product["sku"]),
                "update_type": update_type,
                "effective_at": effective_at.isoformat(),
                "updated_by": "product_feed_system",
            }

            if update_type == "price_change":
                pct = float(self.rng.uniform(*self.file_config.price_change_pct_range))
                old_price = float(product["unit_price"])
                new_price = round_currency(old_price * (1 + pct))
                record.update(
                    {
                        "field_name": "unit_price",
                        "old_value": old_price,
                        "new_value": new_price,
                    }
                )
            elif update_type == "category_change":
                categories = [cat.category for cat in self.config.product_categories]
                new_category = str(self.rng.choice(categories))
                record.update(
                    {
                        "field_name": "category",
                        "old_value": str(product["category"]),
                        "new_value": new_category,
                    }
                )
            elif update_type == "status_change":
                new_status = not bool(product["is_active"])
                record.update(
                    {
                        "field_name": "is_active",
                        "old_value": bool(product["is_active"]),
                        "new_value": new_status,
                    }
                )
            else:
                record.update(
                    {
                        "field_name": "product_name",
                        "old_value": str(product["product_name"]),
                        "new_value": f"{product['product_name']} (Revised)",
                    }
                )

            records.append(record)

        df = pd.DataFrame.from_records(records)
        df["source_system"] = self.file_config.product_updates_source_system
        return df
