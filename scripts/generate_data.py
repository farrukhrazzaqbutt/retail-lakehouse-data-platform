#!/usr/bin/env python3
"""CLI entry point for generating synthetic retail datasets."""

from __future__ import annotations

import sys
from pathlib import Path

import click

# Ensure src is on path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_data_generation_config  # noqa: E402
from retail_lakehouse.pipeline.data_generation import DataGenerationPipeline  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to data_generation.yaml",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory for CSV output (default: DATA_OUTPUT_DIR env var)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed override",
)
@click.option(
    "--customers",
    type=int,
    default=None,
    help="Number of customers to generate",
)
@click.option(
    "--products",
    type=int,
    default=None,
    help="Number of products to generate",
)
@click.option(
    "--orders",
    type=int,
    default=None,
    help="Number of orders to generate",
)
@click.option(
    "--no-export",
    is_flag=True,
    default=False,
    help="Skip CSV export; only run in-memory generation",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    config_path: Path | None,
    output_dir: Path | None,
    seed: int | None,
    customers: int | None,
    products: int | None,
    orders: int | None,
    no_export: bool,
    log_level: str,
) -> None:
    """
    Generate synthetic retail data for customers, products, orders, and payments.

    Example:
        python scripts/generate_data.py --customers 100 --products 50 --orders 500
    """
    configure_logging(log_level)
    config = load_data_generation_config(config_path=config_path)

    # Apply CLI overrides via dataclass replacement pattern
    overrides = {}
    if seed is not None:
        overrides["seed"] = seed
    if customers is not None:
        overrides["num_customers"] = customers
    if products is not None:
        overrides["num_products"] = products
    if orders is not None:
        overrides["num_orders"] = orders
    if overrides:
        from dataclasses import replace

        config = replace(config, **overrides)

    pipeline = DataGenerationPipeline(config)
    datasets = pipeline.run()

    summary = {name: len(df) for name, df in datasets.as_dict().items()}
    logger.info("Generation summary: %s", summary)

    if not no_export:
        pipeline.export_csv(datasets, output_dir=output_dir)
        click.echo(f"CSV files written to {output_dir or config.output_dir}")


if __name__ == "__main__":
    main()
