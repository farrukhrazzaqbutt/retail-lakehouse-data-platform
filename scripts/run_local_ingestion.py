#!/usr/bin/env python3
"""Run local ADF-equivalent ingestion into the raw landing zone."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (  # noqa: E402
    load_adf_ingestion_config,
    load_file_sources_config,
    load_postgres_config,
)
from retail_lakehouse.ingestion.azure_landing import AzureLandingClient  # noqa: E402
from retail_lakehouse.ingestion.local_landing import LocalLandingPipeline  # noqa: E402
from retail_lakehouse.ingestion.postgres_extractor import PostgresExtractor  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--postgres-only", is_flag=True, default=False, help="Ingest PostgreSQL tables only"
)
@click.option(
    "--files-only", is_flag=True, default=False, help="Ingest file sources only"
)
@click.option(
    "--upload-azure",
    is_flag=True,
    default=False,
    help="Upload landed files to Azure Storage after local landing",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    postgres_only: bool, files_only: bool, upload_azure: bool, log_level: str
) -> None:
    """
    Mirror ADF master ingestion pipeline for local development.

    Example:
        python scripts/run_local_ingestion.py
        python scripts/run_local_ingestion.py --upload-azure
    """
    configure_logging(log_level)

    ingestion_config = load_adf_ingestion_config()
    postgres_config = load_postgres_config()
    file_sources_config = load_file_sources_config()

    include_postgres = not files_only
    include_files = not postgres_only

    extractor = PostgresExtractor(postgres_config, ingestion_config)
    pipeline = LocalLandingPipeline(
        ingestion_config=ingestion_config,
        postgres_extractor=extractor,
        file_sources_dir=file_sources_config.landing_dir,
    )
    result = pipeline.run(
        include_postgres=include_postgres, include_files=include_files
    )

    click.echo(f"Ingestion batch_id: {result.batch_id}")
    click.echo(f"Ingestion date:     {result.ingestion_date}")
    click.echo(f"Total rows landed:  {result.total_rows}")
    click.echo("")
    for landing in result.landings:
        click.echo(
            f"  [{landing.source_type}] {landing.entity}: "
            f"{landing.rows_landed} rows -> {landing.landing_path}"
        )

    if upload_azure:
        client = AzureLandingClient.from_environment()
        uploaded = client.upload_directory(
            ingestion_config.local_landing_dir,
            target_prefix=ingestion_config.adls_base_path,
        )
        click.echo(f"\nUploaded {uploaded} files to Azure Storage.")


if __name__ == "__main__":
    main()
