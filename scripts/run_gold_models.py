#!/usr/bin/env python3
"""Run Gold Delta models and business metric marts from Silver tables."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_gold_model_config  # noqa: E402
from retail_lakehouse.gold.pipeline import GoldModelPipeline  # noqa: E402
from retail_lakehouse.spark.session import get_spark_session, stop_spark_session  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option("--batch-id", default=None, help="Batch identifier for manifest logging")
@click.option("--log-level", default="INFO", help="Logging level")
def main(batch_id: str | None, log_level: str) -> None:
    """
    Build Gold dimensions, facts, and business metric marts from Silver Delta.

    Example:
        python scripts/run_gold_models.py
        python scripts/run_gold_models.py --batch-id retail_gold_20260711
    """
    configure_logging(log_level)
    config = load_gold_model_config()

    spark = get_spark_session(warehouse_dir=str(config.gold_root))
    try:
        pipeline = GoldModelPipeline(spark, config)
        result = pipeline.run(batch_id=batch_id)
    finally:
        stop_spark_session(spark)

    click.echo(f"Gold batch_id:   {result.batch_id or 'latest'}")
    click.echo(f"Total rows:      {result.total_rows}")
    click.echo(f"Tables written:  {len(result.tables)}")
    click.echo("")
    for layer in ("dimensions", "facts", "marts"):
        layer_tables = [table for table in result.tables if table.layer == layer]
        if not layer_tables:
            continue
        click.echo(f"  {layer}:")
        for table in layer_tables:
            click.echo(f"    {table.table_name}: rows={table.row_count}")


if __name__ == "__main__":
    main()
