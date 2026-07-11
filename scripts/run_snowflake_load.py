#!/usr/bin/env python3
"""Load Gold Delta tables into Snowflake RAW schema."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (  # noqa: E402
    load_snowflake_config,
    load_snowflake_load_config,
)
from retail_lakehouse.spark.session import (  # noqa: E402
    get_spark_session,
    stop_spark_session,
)
from retail_lakehouse.utils.logging import (  # noqa: E402
    configure_logging,
    get_logger,
)
from retail_lakehouse.warehouse.gold_reader import (  # noqa: E402
    GoldDeltaReader,
)
from retail_lakehouse.warehouse.pipeline import (  # noqa: E402
    SnowflakeLoadPipeline,
)

logger = get_logger(__name__)


@click.command()
@click.option(
    "--batch-id",
    default=None,
    help="Batch identifier for manifest logging",
)
@click.option(
    "--tables",
    default=None,
    help="Comma-separated table list (default: all configured tables)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate Gold source tables without connecting to Snowflake",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    batch_id: str | None,
    tables: str | None,
    dry_run: bool,
    log_level: str,
) -> None:
    """
    Load Gold dimensions, facts, and marts into Snowflake.

    Example:
        python scripts/setup_snowflake.py
        python scripts/run_snowflake_load.py
        python scripts/run_snowflake_load.py --tables dim_customers,fct_orders
        python scripts/run_snowflake_load.py --dry-run
    """
    configure_logging(log_level)
    snowflake_config = load_snowflake_config()
    load_config = load_snowflake_load_config()
    table_list = [item.strip() for item in tables.split(",")] if tables else None

    spark = get_spark_session(
        app_name="retail-lakehouse-snowflake-load",
        warehouse_dir=str(load_config.gold_root),
    )
    try:
        reader = GoldDeltaReader(spark, load_config)

        if dry_run:
            click.echo("Dry run — validating Gold source tables only.\n")
            missing: list[str] = []
            for layer, table_name in reader.list_configured_tables():
                if table_list and table_name not in table_list:
                    continue
                exists = reader.table_exists(layer, table_name)
                status = "FOUND" if exists else "MISSING"
                click.echo(f"  [{status}] {layer}/{table_name}")
                if not exists:
                    missing.append(f"{layer}/{table_name}")

            if missing:
                click.echo("\nDry run failed — missing Gold tables:")
                for item in missing:
                    click.echo(f"  - {item}")
                raise SystemExit(1)

            click.echo("\nDry run passed. Run scripts/setup_snowflake.py then reload.")
            return

        pipeline = SnowflakeLoadPipeline(spark, snowflake_config, load_config)
        result = pipeline.run(batch_id=batch_id, tables=table_list)
    finally:
        stop_spark_session(spark)

    target = f"{snowflake_config.database}.{snowflake_config.schema}"
    click.echo(f"Snowflake batch_id:  {result.batch_id or 'latest'}")
    click.echo(f"Target:             {target}")
    click.echo(f"Total rows loaded:  {result.total_rows}")
    click.echo(f"Tables loaded:      {len(result.tables)}")
    click.echo("")
    for table in result.tables:
        click.echo(f"  {table.layer}/{table.table_name}: rows={table.row_count}")


if __name__ == "__main__":
    main()
