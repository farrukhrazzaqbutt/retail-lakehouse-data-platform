"""Tests for product data generator."""

from __future__ import annotations

from retail_lakehouse.generators.products import ProductGenerator


def test_product_generator_row_count(small_config) -> None:
    df = ProductGenerator(small_config).generate()
    assert len(df) == small_config.num_products


def test_product_generator_unique_sku(small_config) -> None:
    df = ProductGenerator(small_config).generate()
    assert df["sku"].is_unique
    assert df["product_id"].is_unique


def test_product_generator_positive_prices(small_config) -> None:
    df = ProductGenerator(small_config).generate()
    assert (df["unit_price"] > 0).all()
    assert (df["unit_cost"] >= 0).all()
    assert (df["unit_price"] >= df["unit_cost"]).all()


def test_product_generator_categories(small_config) -> None:
    df = ProductGenerator(small_config).generate()
    allowed = {category.category for category in small_config.product_categories}
    assert set(df["category"].unique()).issubset(allowed)
