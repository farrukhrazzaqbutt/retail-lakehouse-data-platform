#!/usr/bin/env python3
"""Validate Gold Delta dimensions, facts, and business metric marts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (  # noqa: E402
    load_gold_model_config,
)
from retail_lakehouse.spark.session import (  # noqa: E402
    get_spark_session,
    stop_spark_session,
)
from retail_lakehouse.utils.logging import (  # noqa: E402
    configure_logging,
    get_logger,
)

logger = get_logger(__name__)

MART_METRIC_COLUMNS: dict[str, list[str]] = {
    "mart_daily_sales": [
        "gross_revenue",
        "net_revenue",
        "order_count",
        "average_order_value",
        "payment_failure_rate",
        "cancellation_rate",
    ],
    "mart_monthly_revenue": [
        "gross_revenue",
        "net_revenue",
        "monthly_active_customers",
        "new_customers",
        "returning_customers",
    ],
    "mart_customer_lifetime_value": [
        "total_orders",
        "total_revenue",
        "net_revenue",
        "avg_order_value",
        "repeat_purchase_rate",
    ],
    "mart_product_performance": [
        "total_quantity_sold",
        "gross_revenue",
        "net_revenue",
    ],
    "mart_customer_segments": [
        "customer_count",
        "total_revenue",
        "net_revenue",
        "repeat_purchase_rate",
        "cancellation_rate",
    ],
}


@click.command()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """Validate Gold layer outputs and business metric marts."""
    configure_logging(log_level)
    config = load_gold_model_config()
    gold_root = config.gold_root / config.gold_base_path

    if not gold_root.exists():
        click.echo(f"Gold layer not found: {gold_root}")
        click.echo(
            "Run scripts/run_silver_transforms.py then "
            "scripts/run_gold_models.py first."
        )
        raise SystemExit(1)

    spark = get_spark_session(warehouse_dir=str(config.gold_root))
    failures: list[str] = []

    try:
        click.echo(f"Validating Gold layer: {gold_root}\n")

        layers: list[tuple[str, list[str]]] = [
            ("dimensions", config.dimensions),
            ("facts", config.facts),
            ("marts", config.marts),
        ]

        for layer, tables in layers:
            click.echo(f"  {layer}:")
            for table_name in tables:
                table_path = gold_root / layer / table_name
                if not table_path.exists():
                    failures.append(f"{table_name}: Gold Delta table missing")
                    click.echo(f"    [FAIL] {table_name}: Delta table missing")
                    continue

                df = spark.read.format("delta").load(str(table_path))
                row_count = df.count()
                has_processed_at = config.processed_at_column in df.columns
                missing_metrics = [
                    col
                    for col in MART_METRIC_COLUMNS.get(table_name, [])
                    if col not in df.columns
                ]

                if row_count == 0 or not has_processed_at or missing_metrics:
                    failures.append(f"{table_name}: schema or row validation failed")
                    click.echo(
                        f"    [FAIL] {table_name}: rows={row_count} "
                        f"processed_at={has_processed_at} "
                        f"missing_metrics={missing_metrics}"
                    )
                else:
                    click.echo(f"    [PASS] {table_name}: rows={row_count}")

        manifest_dir = gold_root / "_manifests"
        manifests = (
            list(manifest_dir.glob("gold_run_*.json")) if manifest_dir.exists() else []
        )
        if manifests:
            latest_path = max(manifests, key=lambda p: p.stat().st_mtime)
            latest = json.loads(latest_path.read_text())
            click.echo(
                f"\n  Latest manifest: batch_id={latest.get('batch_id')} "
                f"total_rows={latest.get('total_rows', 0)}"
            )
        else:
            failures.append("No Gold manifest found")
            click.echo("\n  [FAIL] No Gold manifest found")

    finally:
        stop_spark_session(spark)

    if failures:
        click.echo(f"\nValidation failed ({len(failures)} issues):")
        for failure in failures:
            click.echo(f"  - {failure}")
        raise SystemExit(1)

    click.echo("\nAll Gold validation checks passed.")


if __name__ == "__main__":
    main()
