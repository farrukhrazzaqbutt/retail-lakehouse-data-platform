"""Tests for website event file source generator."""

from __future__ import annotations

from dataclasses import replace

from retail_lakehouse.config.settings import load_file_sources_config
from retail_lakehouse.generators.customers import CustomerGenerator
from retail_lakehouse.generators.products import ProductGenerator
from retail_lakehouse.generators.website_events import WebsiteEventGenerator


def test_website_event_generator_count(small_config) -> None:
    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=10, num_customers=10)
    file_config = replace(
        file_config, data_generation=data_config, website_events_per_file=30
    )

    customers = CustomerGenerator(data_config).generate()
    products = ProductGenerator(data_config).generate()
    events = WebsiteEventGenerator(file_config, customers, products).generate()
    assert len(events) == 30


def test_website_event_generator_valid_fields(small_config) -> None:
    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=10, num_customers=10)
    file_config = replace(
        file_config, data_generation=data_config, website_events_per_file=25
    )

    customers = CustomerGenerator(data_config).generate()
    products = ProductGenerator(data_config).generate()
    events = WebsiteEventGenerator(file_config, customers, products).generate()

    assert all("event_id" in event for event in events)
    assert all("event_type" in event for event in events)
    assert all("source_system" in event for event in events)
    allowed_types = {opt.name for opt in file_config.event_types}
    assert all(event["event_type"] in allowed_types for event in events)


def test_website_event_json_lines(small_config) -> None:
    file_config = load_file_sources_config()
    data_config = replace(small_config, num_products=5, num_customers=5)
    file_config = replace(
        file_config, data_generation=data_config, website_events_per_file=5
    )

    customers = CustomerGenerator(data_config).generate()
    products = ProductGenerator(data_config).generate()
    generator = WebsiteEventGenerator(file_config, customers, products)
    events = generator.generate()
    payload = generator.to_json_lines(events)
    assert payload.count("\n") == len(events) - 1
