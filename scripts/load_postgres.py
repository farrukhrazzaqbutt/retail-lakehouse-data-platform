#!/usr/bin/env python3
"""CLI entry point for loading generated data into PostgreSQL."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (  # noqa: E402
    load_data_generation_config,
    load_postgres_config,
    load_postgres_table_config,
)
from retail_lakehouse.loaders.postgres_loader import PostgresLoader  # noqa: E402
from retail_lakehouse.pipeline.data_generation import DataGenerationPipeline  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--truncate",
    is_flag=True,
    default=False,
    help="Truncate target tables before loading (idempotent full reload)",
)
@click.option(
    "--from-csv",
    "csv_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Load from existing CSV files instead of generating fresh data",
)
@click.option(
    "--customers",
    type=int,
    default=None,
    help="Override number of customers when generating inline",
)
@click.option(
    "--products",
    type=int,
    default=None,
    help="Override number of products when generating inline",
)
@click.option(
    "--orders",
    type=int,
    default=None,
    help="Override number of orders when generating inline",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    truncate: bool,
    csv_dir: Path | None,
    customers: int | None,
    products: int | None,
    orders: int | None,
    log_level: str,
) -> None:
    """
    Generate (or read) synthetic data and load it into PostgreSQL.

    Example:
        python scripts/load_postgres.py --truncate --customers 100 --orders 500
    """
    configure_logging(log_level)

    postgres_config = load_postgres_config()
    table_config = load_postgres_table_config()
    loader = PostgresLoader(postgres_config, table_config)

    if csv_dir:
        import pandas as pd

        datasets = {
            table: pd.read_csv(csv_dir / f"{table}.csv")
            for table in ["customers", "products", "orders", "order_items", "payments"]
        }
        logger.info("Loaded datasets from CSV directory: %s", csv_dir)
    else:
        config = load_data_generation_config()
        overrides = {}
        if customers is not None:
            overrides["num_customers"] = customers
        if products is not None:
            overrides["num_products"] = products
        if orders is not None:
            overrides["num_orders"] = orders
        if overrides:
            from dataclasses import replace

            config = replace(config, **overrides)
        datasets = DataGenerationPipeline(config).run().as_dict()

    results = loader.load_all(datasets, truncate_first=truncate)
    counts = loader.get_row_counts()

    click.echo("Load complete:")
    for table, rows in results.items():
        click.echo(f"  {table}: {rows} rows inserted")
    click.echo("\nPostgreSQL table counts:")
    click.echo(counts.to_string(index=False))


if __name__ == "__main__":
    main()
