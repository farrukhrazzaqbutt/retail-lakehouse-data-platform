"""Tests for Silver data quality engine."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row

from retail_lakehouse.config.settings import load_silver_transform_config
from retail_lakehouse.transforms.quality import DataQualityEngine


def test_validate_entity_routes_nulls_to_quarantine(
    spark_session, silver_test_paths, tmp_path
):
    bronze_root, _ = silver_test_paths
    config = replace(
        load_silver_transform_config(config_path=Path("config/silver_transforms.yaml")),
        bronze_root=bronze_root,
        silver_root=tmp_path / "silver",
    )
    entity = next(item for item in config.entities if item.name == "customers")
    engine = DataQualityEngine(config)

    df = spark_session.createDataFrame(
        [
            Row(
                customer_id=1,
                email="a@example.com",
                first_name="Ann",
                last_name="Lee",
                country_code="US",
                customer_segment="standard",
                batch_id="retail_test",
                source_system="retail_postgres",
                source_file="retail.customers",
                ingested_at="2026-07-11T00:00:00Z",
                ingestion_date="2026-07-11",
            ),
            Row(
                customer_id=2,
                email=None,
                first_name="Bob",
                last_name="Ray",
                country_code="US",
                customer_segment="standard",
                batch_id="retail_test",
                source_system="retail_postgres",
                source_file="retail.customers",
                ingested_at="2026-07-11T00:00:00Z",
                ingestion_date="2026-07-11",
            ),
        ]
    )

    valid, quarantine = engine.validate_entity(df, entity)
    assert valid.count() == 1
    assert quarantine.count() == 1
    assert "null_email" in quarantine.collect()[0][config.rejection_reason_column]


def test_validate_entity_rejects_invalid_segment(
    spark_session, silver_test_paths, tmp_path
):
    config = replace(
        load_silver_transform_config(config_path=Path("config/silver_transforms.yaml")),
        bronze_root=silver_test_paths[0],
        silver_root=tmp_path / "silver",
    )
    entity = next(item for item in config.entities if item.name == "customers")
    engine = DataQualityEngine(config)

    df = spark_session.createDataFrame(
        [
            Row(
                customer_id=1,
                email="a@example.com",
                first_name="Ann",
                last_name="Lee",
                country_code="US",
                customer_segment="vip",
                batch_id="retail_test",
                source_system="retail_postgres",
                source_file="retail.customers",
                ingested_at="2026-07-11T00:00:00Z",
                ingestion_date="2026-07-11",
            )
        ]
    )

    _, quarantine = engine.validate_entity(df, entity)
    assert quarantine.count() == 1
    assert (
        "invalid_customer_segment"
        in quarantine.collect()[0][config.rejection_reason_column]
    )
