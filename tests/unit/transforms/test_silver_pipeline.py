"""Tests for Silver transform pipeline."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from retail_lakehouse.config.settings import load_silver_transform_config
from retail_lakehouse.transforms.pipeline import SilverTransformPipeline


def _write_bronze_parquet(spark, path: Path, rows: list[dict]) -> None:
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(path))


def test_silver_pipeline_customers_products(spark_session, silver_test_paths, tmp_path):
    bronze_root, silver_root = silver_test_paths
    config = replace(
        load_silver_transform_config(config_path=Path("config/silver_transforms.yaml")),
        bronze_root=bronze_root,
        silver_root=silver_root,
    )

    batch_id = "retail_test_batch"
    ingestion_date = "2026-07-11"
    metadata = {
        "batch_id": batch_id,
        "source_system": "retail_postgres",
        "source_file": "retail.customers",
        "ingested_at": "2026-07-11T00:00:00Z",
        "ingestion_date": ingestion_date,
    }

    customers_path = (
        bronze_root
        / config.bronze_base_path
        / "postgres"
        / "customers"
        / f"ingestion_date={ingestion_date}"
        / f"batch_id={batch_id}"
    )
    products_path = (
        bronze_root
        / config.bronze_base_path
        / "postgres"
        / "products"
        / f"ingestion_date={ingestion_date}"
        / f"batch_id={batch_id}"
    )
    customers_path.mkdir(parents=True)
    products_path.mkdir(parents=True)

    _write_bronze_parquet(
        spark_session,
        customers_path,
        [
            {
                "customer_id": 1,
                "email": "ann@example.com",
                "first_name": "Ann",
                "last_name": "Lee",
                "country_code": "US",
                "customer_segment": "standard",
                **metadata,
            }
        ],
    )
    _write_bronze_parquet(
        spark_session,
        products_path,
        [
            {
                "product_id": 10,
                "sku": "SKU-000010",
                "product_name": "Widget",
                "category": "Electronics",
                "unit_price": 99.99,
                "unit_cost": 50.0,
                **{**metadata, "source_file": "retail.products"},
            }
        ],
    )

    manifest_dir = (
        bronze_root
        / config.bronze_base_path
        / "_manifests"
        / f"ingestion_date={ingestion_date}"
    )
    manifest_dir.mkdir(parents=True)
    (manifest_dir / f"batch_id={batch_id}.json").write_text(
        json.dumps({"batch_id": batch_id, "ingestion_date": ingestion_date}),
        encoding="utf-8",
    )

    pipeline = SilverTransformPipeline(spark_session, config)
    result = pipeline.run(
        batch_id=batch_id,
        ingestion_date=ingestion_date,
        entities=["customers", "products"],
    )

    assert result.total_valid_rows == 2
    assert len(result.entities) == 2

    silver_customers = silver_root / config.silver_base_path / "postgres" / "customers"
    assert silver_customers.exists()
    silver_df = spark_session.read.format("delta").load(str(silver_customers))
    assert silver_df.count() == 1
    assert "processed_at" in silver_df.columns


def test_load_silver_transform_config() -> None:
    config = load_silver_transform_config(
        config_path=Path("config/silver_transforms.yaml")
    )
    assert len(config.entities) == 7
    assert config.referential_integrity_enabled is True
