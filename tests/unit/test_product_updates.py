"""Tests for product update file source generator."""

from __future__ import annotations

from retail_lakehouse.generators.product_updates import ProductUpdateGenerator
from retail_lakehouse.generators.products import ProductGenerator


def test_product_update_generator_row_count(small_config) -> None:
    from dataclasses import replace

    from retail_lakehouse.config.settings import load_file_sources_config

    file_config = load_file_sources_config()
    data_config = replace(
        small_config,
        num_products=10,
        num_customers=10,
    )
    file_config = replace(
        file_config, data_generation=data_config, product_updates_records=20
    )

    products = ProductGenerator(data_config).generate()
    df = ProductUpdateGenerator(file_config, products).generate()
    assert len(df) == 20


def test_product_update_generator_valid_product_refs(small_config) -> None:
    from dataclasses import replace

    from retail_lakehouse.config.settings import load_file_sources_config

    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=10)
    file_config = replace(
        file_config, data_generation=data_config, product_updates_records=15
    )

    products = ProductGenerator(data_config).generate()
    df = ProductUpdateGenerator(file_config, products).generate()
    assert set(df["product_id"]).issubset(set(products["product_id"]))
    assert set(df["update_type"].unique()).issubset(
        {opt.name for opt in file_config.product_update_types}
    )


def test_product_update_generator_has_source_system(small_config) -> None:
    from dataclasses import replace

    from retail_lakehouse.config.settings import load_file_sources_config

    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=5)
    file_config = replace(
        file_config, data_generation=data_config, product_updates_records=5
    )
    products = ProductGenerator(data_config).generate()
    df = ProductUpdateGenerator(file_config, products).generate()
    assert (df["source_system"] == file_config.product_updates_source_system).all()
