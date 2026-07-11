"""Tests for file source pipeline orchestration."""

from __future__ import annotations

from dataclasses import replace

from retail_lakehouse.config.settings import load_file_sources_config
from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.products import ProductGenerator
from retail_lakehouse.pipeline.file_sources import FileSourcePipeline


def test_file_source_pipeline_writes_files(small_config, tmp_path) -> None:
    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=10, num_customers=10)
    file_config = replace(
        file_config,
        data_generation=data_config,
        landing_dir=tmp_path / "file_sources",
        product_updates_records=10,
        website_events_per_file=15,
    )

    products = ProductGenerator(data_config).generate()
    customers = CustomerGenerator(data_config).generate()
    outputs = FileSourcePipeline(file_config).run(products, customers)

    assert outputs.product_updates_csv.exists()
    assert outputs.website_events_json.exists()
    assert outputs.product_updates_rows == 10
    assert outputs.website_events_rows == 15
