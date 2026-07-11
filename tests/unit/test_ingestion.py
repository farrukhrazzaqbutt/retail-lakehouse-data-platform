"""Tests for ingestion metadata and configuration."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from retail_lakehouse.config.settings import load_adf_ingestion_config
from retail_lakehouse.ingestion.metadata import (
    IngestionMetadata,
    build_landing_path,
    enrich_dataframe,
)


def test_ingestion_metadata_create() -> None:
    metadata = IngestionMetadata.create(
        batch_id_prefix="retail",
        source_system="retail_postgres",
        source_file="retail.customers",
    )
    assert metadata.batch_id.startswith("retail_")
    assert metadata.source_system == "retail_postgres"
    assert metadata.ingestion_date is not None


def test_enrich_dataframe_adds_metadata_columns() -> None:
    metadata = IngestionMetadata.create(
        batch_id_prefix="retail",
        source_system="test",
        source_file="test.csv",
    )
    df = pd.DataFrame({"id": [1, 2]})
    enriched = enrich_dataframe(df, metadata)
    assert REQUIRED_COLUMNS.issubset(set(enriched.columns))
    assert len(enriched) == 2


def test_build_landing_path() -> None:
    metadata = IngestionMetadata.create(
        batch_id_prefix="retail",
        source_system="retail_postgres",
        source_file="retail.orders",
    )
    path = build_landing_path(
        "{base_path}/{source_type}/{entity}/ingestion_date={ingestion_date}/batch_id={batch_id}",
        base_path="bronze",
        source_type="postgres",
        entity="orders",
        metadata=metadata,
    )
    assert path.startswith("bronze/postgres/orders/ingestion_date=")
    assert f"batch_id={metadata.batch_id}" in path


def test_load_adf_ingestion_config() -> None:
    config = load_adf_ingestion_config(config_path=Path("config/adf_ingestion.yaml"))
    assert config.adls_base_path == "bronze"
    assert len(config.postgres_tables) == 5
    assert len(config.file_sources) == 2
    assert "batch_id" in config.metadata_columns


def test_adf_json_artifacts_are_valid_json() -> None:
    adf_root = Path("adf")
    json_files = list(adf_root.rglob("*.json"))
    assert len(json_files) >= 10
    for path in json_files:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        assert isinstance(payload, dict)


REQUIRED_COLUMNS = {
    "batch_id",
    "source_system",
    "source_file",
    "ingested_at",
    "ingestion_date",
}
