#!/usr/bin/env python3
"""Generate CSV product updates and JSON website event file sources."""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (
    load_data_generation_config,
    load_file_sources_config,
)  # noqa: E402
from retail_lakehouse.generators.customers import CustomerGenerator  # noqa: E402
from retail_lakehouse.generators.products import ProductGenerator  # noqa: E402
from retail_lakehouse.pipeline.file_sources import FileSourcePipeline  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--products", type=int, default=None, help="Number of products for context"
)
@click.option(
    "--customers", type=int, default=None, help="Number of customers for context"
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(products: int | None, customers: int | None, log_level: str) -> None:
    """
    Generate Phase 2 file sources: product update CSV and website event JSON.

    Example:
        python scripts/generate_file_sources.py --products 50 --customers 100
    """
    configure_logging(log_level)
    file_config = load_file_sources_config()
    data_config = load_data_generation_config()

    overrides: dict[str, int] = {}
    if products is not None:
        overrides["num_products"] = products
    if customers is not None:
        overrides["num_customers"] = customers
    if overrides:
        data_config = replace(data_config, **overrides)
        file_config = replace(file_config, data_generation=data_config)

    products_df = ProductGenerator(data_config).generate()
    customers_df = CustomerGenerator(data_config).generate()
    outputs = FileSourcePipeline(file_config).run(products_df, customers_df)

    click.echo("File sources generated:")
    click.echo(
        f"  product_updates: {outputs.product_updates_rows} rows -> {outputs.product_updates_csv}"
    )
    click.echo(
        f"  website_events:  {outputs.website_events_rows} rows -> {outputs.website_events_json}"
    )


if __name__ == "__main__":
    main()
