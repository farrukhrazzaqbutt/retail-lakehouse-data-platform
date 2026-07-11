"""End-to-end synthetic data generation orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from retail_lakehouse.config.settings import DataGenerationConfig
from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.order_items import OrderItemGenerator
from retail_lakehouse.generators.orders import OrderGenerator
from retail_lakehouse.generators.payments import PaymentGenerator
from retail_lakehouse.generators.products import ProductGenerator
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeneratedDatasets:
    """Container for all generated entity DataFrames."""

    customers: pd.DataFrame
    products: pd.DataFrame
    orders: pd.DataFrame
    order_items: pd.DataFrame
    payments: pd.DataFrame

    def as_dict(self) -> dict[str, pd.DataFrame]:
        """Return datasets as a table-keyed dictionary."""
        return {
            "customers": self.customers,
            "products": self.products,
            "orders": self.orders,
            "order_items": self.order_items,
            "payments": self.payments,
        }


class DataGenerationPipeline:
    """Orchestrate entity generation with referential integrity."""

    def __init__(self, config: DataGenerationConfig) -> None:
        """
        Initialize pipeline.

        Args:
            config: Data generation configuration.
        """
        self.config = config

    def run(self) -> GeneratedDatasets:
        """
        Generate all retail entities in dependency order.

        Returns:
            GeneratedDatasets with customers, products, orders, items, payments.
        """
        logger.info(
            "Starting data generation seed=%s customers=%s products=%s orders=%s",
            self.config.seed,
            self.config.num_customers,
            self.config.num_products,
            self.config.num_orders,
        )

        customers = CustomerGenerator(self.config).generate()
        products = ProductGenerator(self.config).generate()
        orders = OrderGenerator(self.config, customers).generate()
        order_items = OrderItemGenerator(self.config, orders, products).generate()
        orders = OrderGenerator.finalize_amounts(orders, order_items)
        payments = PaymentGenerator(self.config, orders).generate()

        datasets = GeneratedDatasets(
            customers=customers,
            products=products,
            orders=orders,
            order_items=order_items,
            payments=payments,
        )
        self._validate_referential_integrity(datasets)
        logger.info("Data generation completed successfully")
        return datasets

    def export_csv(self, datasets: GeneratedDatasets, output_dir: Path | None = None) -> None:
        """
        Export generated datasets to CSV files.

        Args:
            datasets: Generated entity DataFrames.
            output_dir: Optional override for output directory.
        """
        target_dir = ensure_directory(output_dir or self.config.output_dir)
        for table_key, df in datasets.as_dict().items():
            file_path = target_dir / f"{table_key}.csv"
            df.to_csv(file_path, index=False, encoding="utf-8")
            logger.info("Wrote %s rows to %s", len(df), file_path)

    @staticmethod
    def _validate_referential_integrity(datasets: GeneratedDatasets) -> None:
        """
        Validate basic referential integrity before loading.

        Raises:
            ValueError: If foreign-key relationships are violated.
        """
        customer_ids = set(datasets.customers["customer_id"].tolist())
        product_ids = set(datasets.products["product_id"].tolist())
        order_ids = set(datasets.orders["order_id"].tolist())

        invalid_order_customers = set(datasets.orders["customer_id"]) - customer_ids
        if invalid_order_customers:
            raise ValueError("Orders reference unknown customer IDs")

        invalid_item_orders = set(datasets.order_items["order_id"]) - order_ids
        if invalid_item_orders:
            raise ValueError("Order items reference unknown order IDs")

        invalid_item_products = set(datasets.order_items["product_id"]) - product_ids
        if invalid_item_products:
            raise ValueError("Order items reference unknown product IDs")

        invalid_payment_orders = set(datasets.payments["order_id"]) - order_ids
        if invalid_payment_orders:
            raise ValueError("Payments reference unknown order IDs")

        if datasets.order_items.duplicated(subset=["order_item_id"]).any():
            raise ValueError("Duplicate order_item_id values detected")

        logger.debug("Referential integrity checks passed")
