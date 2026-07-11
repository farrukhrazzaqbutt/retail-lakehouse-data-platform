"""Tests for customer data generator."""

from __future__ import annotations

from retail_lakehouse.generators.customers import CustomerGenerator


def test_customer_generator_row_count(small_config) -> None:
    df = CustomerGenerator(small_config).generate()
    assert len(df) == small_config.num_customers


def test_customer_generator_unique_ids_and_emails(small_config) -> None:
    df = CustomerGenerator(small_config).generate()
    assert df["customer_id"].is_unique
    assert df["email"].is_unique


def test_customer_generator_required_fields_not_null(small_config) -> None:
    df = CustomerGenerator(small_config).generate()
    required = [
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "country_code",
        "customer_segment",
        "signup_date",
    ]
    for column in required:
        assert df[column].notna().all(), f"{column} contains nulls"


def test_customer_generator_valid_segments(small_config) -> None:
    df = CustomerGenerator(small_config).generate()
    allowed = {segment.name for segment in small_config.customer_segments}
    assert set(df["customer_segment"].unique()).issubset(allowed)
