"""Gold layer pipeline orchestration."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import GoldModelConfig
from retail_lakehouse.gold.dimensions import (
    build_dim_country,
    build_dim_customers,
    build_dim_date,
    build_dim_products,
)
from retail_lakehouse.gold.facts import (
    build_fct_order_items,
    build_fct_orders,
    build_fct_payments,
)
from retail_lakehouse.gold.gold_writer import GoldWriter
from retail_lakehouse.gold.marts import (
    build_mart_customer_lifetime_value,
    build_mart_customer_segments,
    build_mart_daily_sales,
    build_mart_monthly_revenue,
    build_mart_product_performance,
)
from retail_lakehouse.gold.silver_reader import SilverReader
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


@dataclass
class GoldTableResult:
    """Summary of a single Gold table build."""

    table_name: str
    layer: str
    row_count: int
    output_path: str


@dataclass
class GoldPipelineResult:
    """Summary of a complete Gold pipeline run."""

    batch_id: str | None
    tables: list[GoldTableResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        """Return total rows written across all Gold tables."""
        return sum(table.row_count for table in self.tables)


class GoldModelPipeline:
    """Orchestrate Silver → Gold dimension, fact, and mart builds."""

    def __init__(self, spark: SparkSession, config: GoldModelConfig) -> None:
        """
        Initialize Gold pipeline.

        Args:
            spark: Active Spark session.
            config: Gold model configuration.
        """
        self.spark = spark
        self.config = config
        self.reader = SilverReader(spark, config)
        self.writer = GoldWriter(spark, config)
        self._cache: dict[str, DataFrame] = {}

    def run(self, batch_id: str | None = None) -> GoldPipelineResult:
        """
        Execute the full Gold model pipeline.

        Args:
            batch_id: Optional batch identifier for manifest logging.

        Returns:
            GoldPipelineResult with per-table summaries.
        """
        silver_data = self.reader.read_all_postgres_entities()
        result = GoldPipelineResult(batch_id=batch_id)

        # Dimensions
        dim_date = build_dim_date(self.spark, self.config)
        dim_customers = build_dim_customers(silver_data["customers"], self.config)
        dim_products = build_dim_products(silver_data["products"], self.config)
        dim_country = build_dim_country(silver_data["customers"], self.config)

        self._cache["dim_date"] = dim_date
        self._cache["dim_customers"] = dim_customers
        self._cache["dim_products"] = dim_products
        self._cache["dim_country"] = dim_country

        for name, df, pk in [
            ("dim_date", dim_date, "date_key"),
            ("dim_customers", dim_customers, "customer_key"),
            ("dim_products", dim_products, "product_key"),
            ("dim_country", dim_country, "country_key"),
        ]:
            path = self.writer.write_table(df, name, primary_key=pk, layer="dimensions")
            result.tables.append(self._result(name, "dimensions", df, path))

        # Facts
        fct_orders = build_fct_orders(silver_data["orders"], self.config)
        fct_order_items = build_fct_order_items(silver_data["order_items"], self.config)
        fct_payments = build_fct_payments(silver_data["payments"], self.config)

        self._cache["fct_orders"] = fct_orders
        self._cache["fct_order_items"] = fct_order_items
        self._cache["fct_payments"] = fct_payments

        for name, df, pk in [
            ("fct_orders", fct_orders, "order_key"),
            ("fct_order_items", fct_order_items, "order_item_key"),
            ("fct_payments", fct_payments, "payment_key"),
        ]:
            path = self.writer.write_table(df, name, primary_key=pk, layer="facts")
            result.tables.append(self._result(name, "facts", df, path))

        # Marts
        marts: list[tuple[str, DataFrame, str | None]] = [
            (
                "mart_daily_sales",
                build_mart_daily_sales(fct_orders, fct_payments, self.config),
                "sale_date",
            ),
            (
                "mart_monthly_revenue",
                build_mart_monthly_revenue(fct_orders, self.config),
                "year_month",
            ),
            (
                "mart_customer_lifetime_value",
                build_mart_customer_lifetime_value(fct_orders, self.config),
                "customer_key",
            ),
            (
                "mart_product_performance",
                build_mart_product_performance(
                    fct_order_items, dim_products, fct_orders, self.config
                ),
                "product_id",
            ),
            (
                "mart_customer_segments",
                build_mart_customer_segments(fct_orders, dim_customers, self.config),
                None,
            ),
        ]

        for name, df, mart_pk in marts:
            mode = "merge" if mart_pk else "overwrite"
            path = self.writer.write_table(
                df, name, primary_key=mart_pk, layer="marts", mode=mode
            )
            result.tables.append(self._result(name, "marts", df, path))

        self._write_manifest(result)
        logger.info(
            "Gold pipeline complete tables=%s total_rows=%s",
            len(result.tables),
            result.total_rows,
        )
        return result

    @staticmethod
    def _result(
        table_name: str,
        layer: str,
        df: DataFrame,
        path: str,
    ) -> GoldTableResult:
        """Build a GoldTableResult from a written DataFrame."""
        return GoldTableResult(
            table_name=table_name,
            layer=layer,
            row_count=df.count(),
            output_path=path,
        )

    def _write_manifest(self, result: GoldPipelineResult) -> None:
        """Persist a JSON manifest describing the Gold pipeline run."""
        manifest_dir = ensure_directory(
            self.config.gold_root / self.config.gold_base_path / "_manifests"
        )
        suffix = result.batch_id or "latest"
        payload: dict[str, Any] = {
            "batch_id": result.batch_id,
            "total_rows": result.total_rows,
            "tables": [
                {
                    "table_name": table.table_name,
                    "layer": table.layer,
                    "row_count": table.row_count,
                    "output_path": table.output_path,
                }
                for table in result.tables
            ],
        }
        manifest_path = manifest_dir / f"gold_run_{suffix}.json"
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
