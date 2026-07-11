#!/usr/bin/env python3
"""Validate Silver Delta tables and quarantine outputs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_silver_transform_config  # noqa: E402
from retail_lakehouse.spark.session import get_spark_session, stop_spark_session  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """Validate Silver layer outputs and quarantine tables."""
    configure_logging(log_level)
    config = load_silver_transform_config()
    silver_root = config.silver_root / config.silver_base_path

    if not silver_root.exists():
        click.echo(f"Silver layer not found: {silver_root}")
        click.echo("Run scripts/run_silver_transforms.py first.")
        raise SystemExit(1)

    spark = get_spark_session(warehouse_dir=str(config.silver_root))
    failures: list[str] = []

    try:
        click.echo(f"Validating Silver layer: {silver_root}\n")

        for entity in config.entities:
            entity_path = silver_root / entity.source_type / entity.name
            if not entity_path.exists():
                failures.append(f"{entity.name}: Silver Delta table missing")
                click.echo(f"  [FAIL] {entity.name}: Silver Delta table missing")
                continue

            df = spark.read.format("delta").load(str(entity_path))
            row_count = df.count()
            missing_required = [
                col for col in entity.required_columns if col not in df.columns
            ]
            has_processed_at = config.processed_at_column in df.columns

            if missing_required or not has_processed_at:
                failures.append(f"{entity.name}: schema incomplete")
                click.echo(
                    f"  [FAIL] {entity.name}: rows={row_count} "
                    f"missing={missing_required} processed_at={has_processed_at}"
                )
            else:
                click.echo(f"  [PASS] {entity.name}: rows={row_count}")

        manifest_dir = silver_root / "_manifests"
        manifests = (
            list(manifest_dir.glob("**/*.json")) if manifest_dir.exists() else []
        )
        if manifests:
            latest = json.loads(
                max(manifests, key=lambda p: p.stat().st_mtime).read_text()
            )
            click.echo(
                f"\n  Latest manifest: batch_id={latest['batch_id']} "
                f"quarantine_rows={latest.get('total_quarantine_rows', 0)}"
            )
        else:
            failures.append("No Silver manifest found")
            click.echo("\n  [FAIL] No Silver manifest found")

        quarantine_root = config.silver_root / config.quarantine_base_path
        if quarantine_root.exists():
            quarantine_tables = list(quarantine_root.glob("*"))
            click.echo(f"  Quarantine entities: {len(quarantine_tables)}")
        else:
            click.echo("  Quarantine: none (all records valid)")

    finally:
        stop_spark_session(spark)

    if failures:
        click.echo(f"\nValidation failed ({len(failures)} issues):")
        for failure in failures:
            click.echo(f"  - {failure}")
        raise SystemExit(1)

    click.echo("\nAll Silver validation checks passed.")


if __name__ == "__main__":
    main()
