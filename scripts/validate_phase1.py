#!/usr/bin/env python3
"""Validate Phase 1 data quality and referential integrity in PostgreSQL."""

from __future__ import annotations

import sys
from pathlib import Path

import click
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_postgres_config  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)

VALIDATION_QUERIES: dict[str, str] = {
    "orphan_orders": """
        SELECT COUNT(*) AS violation_count
        FROM retail.orders o
        LEFT JOIN retail.customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,
    "orphan_order_items_orders": """
        SELECT COUNT(*) AS violation_count
        FROM retail.order_items oi
        LEFT JOIN retail.orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,
    "orphan_order_items_products": """
        SELECT COUNT(*) AS violation_count
        FROM retail.order_items oi
        LEFT JOIN retail.products p ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
    """,
    "orphan_payments": """
        SELECT COUNT(*) AS violation_count
        FROM retail.payments p
        LEFT JOIN retail.orders o ON p.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,
    "negative_order_totals": """
        SELECT COUNT(*) AS violation_count
        FROM retail.orders
        WHERE total_amount < 0 OR subtotal_amount < 0
    """,
    "duplicate_customer_emails": """
        SELECT COUNT(*) AS violation_count
        FROM (
            SELECT email FROM retail.customers GROUP BY email HAVING COUNT(*) > 1
        ) d
    """,
    "orders_without_items": """
        SELECT COUNT(*) AS violation_count
        FROM retail.orders o
        LEFT JOIN retail.order_items oi ON o.order_id = oi.order_id
        WHERE oi.order_item_id IS NULL
    """,
}


@click.command()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """Run PostgreSQL validation checks for Phase 1."""
    configure_logging(log_level)
    config = load_postgres_config()
    engine = create_engine(config.sqlalchemy_url, pool_pre_ping=True)

    failures: list[str] = []
    with engine.connect() as connection:
        counts = connection.execute(
            text("SELECT * FROM retail.v_table_counts")
        ).fetchall()
        click.echo("Table row counts:")
        for row in counts:
            click.echo(f"  {row.table_name}: {row.row_count}")

        click.echo("\nData quality checks:")
        for check_name, query in VALIDATION_QUERIES.items():
            result = connection.execute(text(query)).scalar_one()
            status = "PASS" if result == 0 else "FAIL"
            click.echo(f"  [{status}] {check_name}: {result} violations")
            if result != 0:
                failures.append(check_name)

    if failures:
        click.echo(f"\nValidation failed: {', '.join(failures)}")
        raise SystemExit(1)

    click.echo("\nAll Phase 1 validation checks passed.")


if __name__ == "__main__":
    main()
