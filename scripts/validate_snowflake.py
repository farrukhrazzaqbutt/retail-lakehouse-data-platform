#!/usr/bin/env python3
"""Validate Snowflake RAW tables against Gold Delta sources."""

from __future__ import annotations

import json
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
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402
from retail_lakehouse.warehouse.gold_reader import GoldDeltaReader  # noqa: E402
from retail_lakehouse.warehouse.loader import SnowflakeLoader  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """Validate Snowflake tables exist and row counts match Gold Delta."""
    configure_logging(log_level)
    snowflake_config = load_snowflake_config()
    load_config = load_snowflake_load_config()

    spark = get_spark_session(
        app_name="retail-lakehouse-snowflake-validate",
        warehouse_dir=str(load_config.gold_root),
    )
    loader = SnowflakeLoader(snowflake_config, load_config)
    reader = GoldDeltaReader(spark, load_config)
    failures: list[str] = []

    try:
        click.echo(
            f"Validating Snowflake target: "
            f"{snowflake_config.database}.{snowflake_config.schema}\n"
        )

        for layer, table_name in reader.list_configured_tables():
            if not loader.table_exists(table_name):
                failures.append(f"{table_name}: Snowflake table missing")
                click.echo(f"  [FAIL] {table_name}: Snowflake table missing")
                continue

            if not reader.table_exists(layer, table_name):
                failures.append(f"{table_name}: Gold source table missing")
                click.echo(f"  [FAIL] {table_name}: Gold source missing")
                continue

            gold_count = reader.read_table(layer, table_name).count()
            snowflake_count = loader.table_row_count(table_name)

            if gold_count != snowflake_count:
                failures.append(
                    f"{table_name}: row count mismatch "
                    f"gold={gold_count} snowflake={snowflake_count}"
                )
                click.echo(
                    f"  [FAIL] {table_name}: gold={gold_count} "
                    f"snowflake={snowflake_count}"
                )
            else:
                click.echo(f"  [PASS] {table_name}: rows={snowflake_count}")

        manifest_dir = load_config.manifest_root / load_config.manifest_base_path
        manifests = (
            list(manifest_dir.glob("snowflake_run_*.json"))
            if manifest_dir.exists()
            else []
        )
        if manifests:
            latest_path = max(manifests, key=lambda p: p.stat().st_mtime)
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
            click.echo(
                f"\n  Latest manifest: batch_id={latest.get('batch_id')} "
                f"total_rows={latest.get('total_rows', 0)}"
            )
        else:
            failures.append("No Snowflake load manifest found")
            click.echo("\n  [FAIL] No Snowflake load manifest found")

    finally:
        stop_spark_session(spark)

    if failures:
        click.echo(f"\nValidation failed ({len(failures)} issues):")
        for failure in failures:
            click.echo(f"  - {failure}")
        raise SystemExit(1)

    click.echo("\nAll Snowflake validation checks passed.")


if __name__ == "__main__":
    main()
