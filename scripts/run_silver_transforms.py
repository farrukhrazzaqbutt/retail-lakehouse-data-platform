#!/usr/bin/env python3
"""Run PySpark Silver transformations with quarantine handling."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_silver_transform_config  # noqa: E402
from retail_lakehouse.spark.session import get_spark_session, stop_spark_session  # noqa: E402
from retail_lakehouse.transforms.pipeline import SilverTransformPipeline  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option("--batch-id", default=None, help="Override batch identifier")
@click.option(
    "--ingestion-date", default=None, help="Override ingestion date partition"
)
@click.option(
    "--entities",
    default=None,
    help="Comma-separated entity list (default: all in dependency order)",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    batch_id: str | None,
    ingestion_date: str | None,
    entities: str | None,
    log_level: str,
) -> None:
    """
    Transform Bronze data into Silver Delta tables with quarantine routing.

    Example:
        python scripts/run_silver_transforms.py
        python scripts/run_silver_transforms.py --entities customers,orders
    """
    configure_logging(log_level)
    config = load_silver_transform_config()
    entity_list = [item.strip() for item in entities.split(",")] if entities else None

    spark = get_spark_session(warehouse_dir=str(config.silver_root))
    try:
        pipeline = SilverTransformPipeline(spark, config)
        result = pipeline.run(
            batch_id=batch_id,
            ingestion_date=ingestion_date,
            entities=entity_list,
        )
    finally:
        stop_spark_session(spark)

    click.echo(f"Silver batch_id:      {result.batch_id}")
    click.echo(f"Ingestion date:       {result.ingestion_date}")
    click.echo(f"Total valid rows:     {result.total_valid_rows}")
    click.echo(f"Total quarantine rows:{result.total_quarantine_rows}")
    click.echo("")
    for entity in result.entities:
        click.echo(
            f"  {entity.entity}: valid={entity.valid_rows} "
            f"quarantine={entity.quarantine_rows}"
        )


if __name__ == "__main__":
    main()
