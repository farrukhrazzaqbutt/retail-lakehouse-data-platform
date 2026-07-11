"""Tests for end-to-end data generation pipeline."""

from __future__ import annotations

from retail_lakehouse.pipeline.data_generation import DataGenerationPipeline


def test_pipeline_generates_all_entities(small_config) -> None:
    datasets = DataGenerationPipeline(small_config).run()
    assert len(datasets.customers) == small_config.num_customers
    assert len(datasets.products) == small_config.num_products
    assert len(datasets.orders) == small_config.num_orders
    assert len(datasets.payments) == small_config.num_orders
    assert len(datasets.order_items) >= small_config.num_orders


def test_pipeline_is_reproducible_with_same_seed(small_config) -> None:
    first = DataGenerationPipeline(small_config).run()
    second = DataGenerationPipeline(small_config).run()
    assert first.customers.equals(second.customers)
    assert first.products.equals(second.products)


def test_pipeline_export_csv(small_config, tmp_path) -> None:
    pipeline = DataGenerationPipeline(small_config)
    datasets = pipeline.run()
    output_dir = tmp_path / "csv"
    pipeline.export_csv(datasets, output_dir=output_dir)

    for table in datasets.as_dict():
        assert (output_dir / f"{table}.csv").exists()
