"""Tests for order item data generator."""

from __future__ import annotations

from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.order_items import OrderItemGenerator
from retail_lakehouse.generators.orders import OrderGenerator
from retail_lakehouse.generators.products import ProductGenerator


def _build_order_items(small_config):
    customers = CustomerGenerator(small_config).generate()
    products = ProductGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    return OrderItemGenerator(small_config, orders, products).generate()


def test_order_items_reference_valid_orders_and_products(small_config) -> None:
    customers = CustomerGenerator(small_config).generate()
    products = ProductGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    order_items = OrderItemGenerator(small_config, orders, products).generate()

    assert set(order_items["order_id"]).issubset(set(orders["order_id"]))
    assert set(order_items["product_id"]).issubset(set(products["product_id"]))


def test_order_items_positive_quantity_and_prices(small_config) -> None:
    order_items = _build_order_items(small_config)
    assert (order_items["quantity"] > 0).all()
    assert (order_items["unit_price"] > 0).all()
    assert (order_items["line_total"] >= 0).all()


def test_order_items_unique_ids(small_config) -> None:
    order_items = _build_order_items(small_config)
    assert order_items["order_item_id"].is_unique
    assert len(order_items) >= small_config.num_orders
