"""Shared pytest fixtures."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest


@pytest.fixture
def small_config():
    """Minimal configuration for fast unit tests."""
    from retail_lakehouse.config.settings import (
        CountryConfig,
        DataGenerationConfig,
        ProductCategoryConfig,
        WeightedOption,
    )

    return DataGenerationConfig(
        seed=42,
        num_customers=25,
        num_products=10,
        num_orders=40,
        order_start_date=date(2024, 1, 1),
        order_end_date=date(2024, 6, 30),
        customer_segments=[
            WeightedOption(name="budget", weight=0.4),
            WeightedOption(name="standard", weight=0.4),
            WeightedOption(name="premium", weight=0.2),
        ],
        order_statuses=[
            WeightedOption(name="completed", weight=0.7),
            WeightedOption(name="cancelled", weight=0.1),
            WeightedOption(name="refunded", weight=0.1),
            WeightedOption(name="pending", weight=0.05),
            WeightedOption(name="failed", weight=0.05),
        ],
        payment_methods=[
            WeightedOption(name="credit_card", weight=0.6),
            WeightedOption(name="paypal", weight=0.4),
        ],
        payment_statuses=[
            WeightedOption(name="succeeded", weight=0.9),
            WeightedOption(name="failed", weight=0.1),
        ],
        product_categories=[
            ProductCategoryConfig(
                category="Electronics",
                subcategories=["Phones", "Laptops"],
                price_range=(50.0, 500.0),
                weight=0.6,
            ),
            ProductCategoryConfig(
                category="Clothing",
                subcategories=["Men", "Women"],
                price_range=(20.0, 150.0),
                weight=0.4,
            ),
        ],
        countries=[
            CountryConfig(code="US", name="United States", weight=0.6),
            CountryConfig(code="GB", name="United Kingdom", weight=0.4),
        ],
        min_items_per_order=1,
        max_items_per_order=3,
        discount_probability=0.2,
        max_discount_pct=0.25,
        output_dir=Path("./data/generated"),
        sample_output_dir=Path("./data/samples"),
    )


@pytest.fixture
def generated_datasets(small_config):
    """Run the full generation pipeline with small volumes."""
    from retail_lakehouse.pipeline.data_generation import DataGenerationPipeline

    return DataGenerationPipeline(small_config).run()


@pytest.fixture(scope="session")
def spark_session():
    """Create one shared Spark session for all Delta transform tests."""
    import os
    import shutil
    import tempfile

    pytest.importorskip("pyspark")
    if not shutil.which("java") and not os.getenv("JAVA_HOME"):
        pytest.skip("Java not installed — Spark tests skipped")

    from retail_lakehouse.spark.session import get_spark_session, stop_spark_session

    warehouse = tempfile.mkdtemp(prefix="retail-lakehouse-spark-")
    spark = get_spark_session(
        app_name="retail-lakehouse-tests",
        warehouse_dir=warehouse,
    )
    yield spark
    stop_spark_session(spark)
    shutil.rmtree(warehouse, ignore_errors=True)


def pytest_collection_modifyitems(items) -> None:
    """Mark Spark-dependent tests explicitly by filename."""
    spark_files = {
        "test_silver_pipeline.py",
        "test_quality.py",
        "test_gold_pipeline.py",
        "test_snowflake_pipeline.py",
        "test_snowflake_ddl.py",
    }
    for item in items:
        if item.path.name in spark_files:
            item.add_marker(pytest.mark.spark)


@pytest.fixture
def silver_test_paths(tmp_path):
    """Bronze and Silver roots for isolated transform tests."""
    bronze_root = tmp_path / "raw"
    silver_root = tmp_path / "silver"
    bronze_root.mkdir()
    silver_root.mkdir()
    return bronze_root, silver_root
