"""Tests for Gold model pipeline."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from retail_lakehouse.config.settings import load_gold_model_config
from retail_lakehouse.gold.pipeline import GoldModelPipeline


def _write_silver_delta(spark, path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").save(str(path))


def _seed_silver_tables(spark, silver_root: Path, silver_base_path: str) -> None:
    base = silver_root / silver_base_path / "postgres"
    _write_silver_delta(
        spark,
        base / "customers",
        [
            {
                "customer_id": 1,
                "email": "ann@example.com",
                "first_name": "Ann",
                "last_name": "Lee",
                "country_code": "US",
                "country_name": "United States",
                "city": "Boston",
                "customer_segment": "standard",
                "signup_date": "2024-01-15",
                "is_active": True,
            },
            {
                "customer_id": 2,
                "email": "bob@example.com",
                "first_name": "Bob",
                "last_name": "Kim",
                "country_code": "GB",
                "country_name": "United Kingdom",
                "city": "London",
                "customer_segment": "premium",
                "signup_date": "2024-02-01",
                "is_active": True,
            },
        ],
    )
    _write_silver_delta(
        spark,
        base / "products",
        [
            {
                "product_id": 10,
                "sku": "SKU-000010",
                "product_name": "Widget",
                "category": "Electronics",
                "subcategory": "Phones",
                "brand": "Acme",
                "unit_price": 99.99,
                "unit_cost": 50.0,
                "is_active": True,
            }
        ],
    )
    _write_silver_delta(
        spark,
        base / "orders",
        [
            {
                "order_id": 100,
                "customer_id": 1,
                "order_date": "2024-03-01T10:00:00Z",
                "order_status": "completed",
                "shipping_country": "US",
                "subtotal_amount": 90.0,
                "discount_amount": 0.0,
                "shipping_amount": 5.0,
                "tax_amount": 5.0,
                "total_amount": 100.0,
            },
            {
                "order_id": 101,
                "customer_id": 2,
                "order_date": "2024-03-15T12:00:00Z",
                "order_status": "cancelled",
                "shipping_country": "GB",
                "subtotal_amount": 50.0,
                "discount_amount": 0.0,
                "shipping_amount": 0.0,
                "tax_amount": 0.0,
                "total_amount": 50.0,
            },
        ],
    )
    _write_silver_delta(
        spark,
        base / "order_items",
        [
            {
                "order_item_id": 1000,
                "order_id": 100,
                "product_id": 10,
                "quantity": 1,
                "unit_price": 99.99,
                "discount_pct": 0.0,
                "line_total": 99.99,
            }
        ],
    )
    _write_silver_delta(
        spark,
        base / "payments",
        [
            {
                "payment_id": 500,
                "order_id": 100,
                "payment_date": "2024-03-01T10:05:00Z",
                "payment_method": "credit_card",
                "payment_status": "succeeded",
                "amount": 100.0,
            },
            {
                "payment_id": 501,
                "order_id": 101,
                "payment_date": "2024-03-15T12:05:00Z",
                "payment_method": "paypal",
                "payment_status": "failed",
                "amount": 50.0,
            },
        ],
    )


def test_gold_pipeline_builds_all_tables(spark_session, tmp_path):
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    config = replace(
        load_gold_model_config(config_path=Path("config/gold_models.yaml")),
        silver_root=silver_root,
        gold_root=gold_root,
        date_start="2024-01-01",
        date_end="2024-03-31",
    )
    _seed_silver_tables(spark_session, silver_root, config.silver_base_path)

    pipeline = GoldModelPipeline(spark_session, config)
    result = pipeline.run(batch_id="gold_test_batch")

    assert len(result.tables) == 12
    assert result.total_rows > 0

    gold_base = gold_root / config.gold_base_path
    daily_sales = spark_session.read.format("delta").load(
        str(gold_base / "marts" / "mart_daily_sales")
    )
    assert daily_sales.count() >= 1
    assert "net_revenue" in daily_sales.columns
    assert "payment_failure_rate" in daily_sales.columns

    clv = spark_session.read.format("delta").load(
        str(gold_base / "marts" / "mart_customer_lifetime_value")
    )
    assert clv.count() == 2

    manifest_path = gold_base / "_manifests" / "gold_run_gold_test_batch.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["batch_id"] == "gold_test_batch"
    assert manifest["total_rows"] == result.total_rows
