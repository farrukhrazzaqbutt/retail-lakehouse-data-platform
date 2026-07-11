#!/usr/bin/env python3
"""Validate Phase 2 ingestion outputs in the local raw landing zone."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (
    load_adf_ingestion_config,
    load_postgres_config,
)  # noqa: E402
from retail_lakehouse.ingestion.postgres_extractor import PostgresExtractor  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)

REQUIRED_METADATA = {
    "batch_id",
    "source_system",
    "source_file",
    "ingested_at",
    "ingestion_date",
}


@click.command()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str) -> None:
    """Validate landed raw data against source counts and metadata requirements."""
    configure_logging(log_level)
    ingestion_config = load_adf_ingestion_config()
    postgres_config = load_postgres_config()
    extractor = PostgresExtractor(postgres_config, ingestion_config)

    landing_root = ingestion_config.local_landing_dir / ingestion_config.adls_base_path
    if not landing_root.exists():
        click.echo(f"Landing zone not found: {landing_root}")
        click.echo("Run scripts/run_local_ingestion.py first.")
        raise SystemExit(1)

    failures: list[str] = []
    click.echo(f"Validating landing zone: {landing_root}\n")

    for table in ingestion_config.postgres_tables:
        source_count = extractor.get_row_count(table.entity)
        parquet_files = list(landing_root.glob(f"postgres/{table.entity}/**/*.parquet"))
        if not parquet_files:
            failures.append(f"postgres.{table.entity}: no parquet files found")
            continue

        latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
        df = pd.read_parquet(latest)
        landed_count = len(df)
        missing_meta = REQUIRED_METADATA - set(df.columns)

        status = "PASS" if landed_count == source_count and not missing_meta else "FAIL"
        click.echo(
            f"  [{status}] postgres.{table.entity}: source={source_count} "
            f"landed={landed_count} metadata_missing={sorted(missing_meta)}"
        )
        if status == "FAIL":
            if landed_count != source_count:
                failures.append(
                    f"postgres.{table.entity}: row count mismatch "
                    f"(source={source_count}, landed={landed_count})"
                )
            if missing_meta:
                failures.append(
                    f"postgres.{table.entity}: missing metadata columns {sorted(missing_meta)}"
                )

    for source in ingestion_config.file_sources:
        enriched_files = list(landing_root.glob(f"file/{source.entity}/**/*_enriched*"))
        if not enriched_files:
            failures.append(f"file.{source.entity}: no enriched files found")
            click.echo(f"  [FAIL] file.{source.entity}: no enriched files found")
            continue

        total_records = 0
        for path in enriched_files:
            if path.suffix == ".csv":
                total_records += len(pd.read_csv(path))
            elif path.suffix == ".json":
                payload = json.loads(path.read_text(encoding="utf-8"))
                total_records += len(payload) if isinstance(payload, list) else 1

        click.echo(
            f"  [PASS] file.{source.entity}: {len(enriched_files)} files, "
            f"{total_records} records with enrichment"
        )

    manifests = list((landing_root / "_manifests").glob("**/*.json"))
    if manifests:
        click.echo(f"\n  Manifest files found: {len(manifests)}")
    else:
        failures.append("No ingestion manifest files found")
        click.echo("\n  [FAIL] No ingestion manifest files found")

    if failures:
        click.echo(f"\nValidation failed ({len(failures)} issues):")
        for failure in failures:
            click.echo(f"  - {failure}")
        raise SystemExit(1)

    click.echo("\nAll Phase 2 validation checks passed.")


if __name__ == "__main__":
    main()
