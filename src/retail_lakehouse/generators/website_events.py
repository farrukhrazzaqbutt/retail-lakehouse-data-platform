"""Website analytics event generator (JSON file source for Phase 2)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pandas as pd

from retail_lakehouse.config.settings import FileSourcesConfig
from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import weighted_choice


class WebsiteEventGenerator(BaseGenerator):
    """Generate website behavioural events delivered as JSON files."""

    def __init__(
        self,
        config: FileSourcesConfig,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
    ) -> None:
        """
        Initialize website event generator.

        Args:
            config: File source configuration.
            customers_df: Customers for session attribution.
            products_df: Products for product-related events.
        """
        super().__init__(config.data_generation)
        self.file_config = config
        self.customers_df = customers_df
        self.products_df = products_df
        if customers_df.empty:
            raise ValueError("customers_df must not be empty")

    def generate(self, num_events: int | None = None) -> list[dict[str, Any]]:
        """
        Generate website event records.

        Args:
            num_events: Optional override for number of events.

        Returns:
            List of event dictionaries (JSON-serializable).
        """
        count = num_events or self.file_config.website_events_per_file
        base_time = self.audit_timestamp()
        events: list[dict[str, Any]] = []

        for event_id in range(1, count + 1):
            customer = self.customers_df.iloc[
                int(self.rng.integers(0, len(self.customers_df)))
            ]
            event_type = weighted_choice(self.rng, self.file_config.event_types)
            device = weighted_choice(self.rng, self.file_config.device_types)
            channel = weighted_choice(self.rng, self.file_config.channels)
            event_time = base_time + timedelta(seconds=int(self.rng.integers(0, 86400)))

            event: dict[str, Any] = {
                "event_id": f"evt_{event_id:08d}",
                "event_type": event_type,
                "event_timestamp": event_time.isoformat(),
                "customer_id": int(customer["customer_id"]),
                "session_id": f"sess_{int(customer['customer_id']):06d}_{event_id // 10}",
                "device_type": device,
                "traffic_channel": channel,
                "country_code": str(customer["country_code"]),
                "page_url": self._build_page_url(event_type),
                "source_system": self.file_config.website_events_source_system,
            }

            if event_type in {"product_view", "add_to_cart", "purchase"}:
                product = self.products_df.iloc[
                    int(self.rng.integers(0, len(self.products_df)))
                ]
                event["product_id"] = int(product["product_id"])
                event["sku"] = str(product["sku"])
                event["product_name"] = str(product["product_name"])

            if event_type == "search":
                event["search_query"] = self.faker.word()

            if event_type == "purchase":
                event["order_value"] = float(self.rng.uniform(25.0, 500.0))

            events.append(event)

        return events

    @staticmethod
    def _build_page_url(event_type: str) -> str:
        """Return a realistic page URL for the event type."""
        routes = {
            "page_view": "/home",
            "product_view": "/products/detail",
            "add_to_cart": "/cart",
            "checkout_start": "/checkout",
            "purchase": "/checkout/confirmation",
            "search": "/search",
        }
        return f"https://shop.example-retail.com{routes.get(event_type, '/home')}"

    def to_json_lines(self, events: list[dict[str, Any]]) -> str:
        """Serialize events as newline-delimited JSON."""
        return "\n".join(json.dumps(event, default=str) for event in events)
