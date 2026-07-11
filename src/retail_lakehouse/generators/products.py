"""Product synthetic data generator."""

from __future__ import annotations

import pandas as pd

from retail_lakehouse.config.settings import ProductCategoryConfig
from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import (
    choose_weighted,
    generate_id_sequence,
    round_currency,
)


class ProductGenerator(BaseGenerator):
    """Generate product catalog records with category-aware pricing."""

    def generate(self) -> pd.DataFrame:
        """
        Generate product catalog records.

        Returns:
            DataFrame aligned to ``retail.products`` schema.
        """
        count = self.config.num_products
        product_ids = generate_id_sequence(1, count)
        records: list[dict[str, object]] = []

        for product_id in product_ids:
            category = self._choose_category()
            subcategory = str(self.rng.choice(category.subcategories))
            unit_price = round_currency(
                float(
                    self.rng.uniform(
                        category.price_range[0],
                        category.price_range[1],
                    )
                )
            )
            margin = float(self.rng.uniform(0.15, 0.55))
            unit_cost = round_currency(unit_price * (1 - margin))
            brand = self.faker.company()
            product_name = self._build_product_name(
                category.category, subcategory, brand
            )

            records.append(
                {
                    "product_id": int(product_id),
                    "sku": f"SKU-{int(product_id):06d}",
                    "product_name": product_name,
                    "category": category.category,
                    "subcategory": subcategory,
                    "brand": brand,
                    "unit_price": unit_price,
                    "unit_cost": unit_cost,
                    "currency": "USD",
                    "is_active": bool(self.rng.random() > 0.03),
                }
            )

        df = pd.DataFrame.from_records(records)
        return self._add_audit_columns(df)

    def _choose_category(self) -> ProductCategoryConfig:
        """Select a product category using configured weights."""
        return choose_weighted(
            self.rng,
            self.config.product_categories,
            [category.weight for category in self.config.product_categories],
        )

    def _build_product_name(self, category: str, subcategory: str, brand: str) -> str:
        """Compose a readable product title."""
        descriptor = self.faker.word().title()
        return f"{brand} {subcategory} {descriptor}"
