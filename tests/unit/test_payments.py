"""Tests for payment data generator."""

from __future__ import annotations

from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.order_items import OrderItemGenerator
from retail_lakehouse.generators.orders import OrderGenerator
from retail_lakehouse.generators.payments import PaymentGenerator
from retail_lakehouse.generators.products import ProductGenerator


def _build_payments(small_config):
    customers = CustomerGenerator(small_config).generate()
    products = ProductGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    order_items = OrderItemGenerator(small_config, orders, products).generate()
    orders = OrderGenerator.finalize_amounts(orders, order_items)
    return PaymentGenerator(small_config, orders).generate()


def test_payment_generator_one_per_order(small_config) -> None:
    payments = _build_payments(small_config)
    assert len(payments) == small_config.num_orders


def test_payment_generator_valid_order_references(small_config) -> None:
    customers = CustomerGenerator(small_config).generate()
    products = ProductGenerator(small_config).generate()
    orders = OrderGenerator(small_config, customers).generate()
    order_items = OrderItemGenerator(small_config, orders, products).generate()
    orders = OrderGenerator.finalize_amounts(orders, order_items)
    payments = PaymentGenerator(small_config, orders).generate()

    assert set(payments["order_id"]).issubset(set(orders["order_id"]))


def test_payment_generator_unique_transaction_refs(small_config) -> None:
    payments = _build_payments(small_config)
    assert payments["transaction_ref"].is_unique
    assert payments["payment_id"].is_unique


def test_payment_amounts_non_negative(small_config) -> None:
    payments = _build_payments(small_config)
    assert (payments["amount"] >= 0).all()
