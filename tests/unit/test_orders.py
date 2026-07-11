"""Tests for order data generator."""

from __future__ import annotations

from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.order_items import OrderItemGenerator
from retail_lakehouse.generators.orders import OrderGenerator
from retail_lakehouse.generators.products import ProductGenerator


def test_order_generator_row_count(small_config) -> None:
    customers = CustomerGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    assert len(orders) == small_config.num_orders


def test_order_generator_valid_customer_references(small_config) -> None:
    customers = CustomerGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    customer_ids = set(customers["customer_id"])
    assert set(orders["customer_id"]).issubset(customer_ids)


def test_finalize_amounts_populates_totals(small_config) -> None:
    customers = CustomerGenerator(small_config).generate()
    products = ProductGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    order_items = OrderItemGenerator(small_config, orders, products).generate()
    finalized = OrderGenerator.finalize_amounts(orders, order_items)

    assert (finalized["subtotal_amount"] >= 0).all()
    assert (finalized["total_amount"] >= 0).all()
    assert (finalized["tax_amount"] >= 0).all()
    assert finalized["total_amount"].notna().all()
