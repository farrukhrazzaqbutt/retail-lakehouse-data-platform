"""File source generation orchestration for Phase 2."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from retail_lakehouse.config.settings import FileSourcesConfig
from retail_lakehouse.generators.product_updates import ProductUpdateGenerator
from retail_lakehouse.generators.website_events import WebsiteEventGenerator
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileSourceOutputs:
    """Paths to generated file source artifacts."""

    product_updates_csv: Path
    website_events_json: Path
    product_updates_rows: int
    website_events_rows: int


class FileSourcePipeline:
    """Generate CSV product updates and JSON website event files."""

    def __init__(self, config: FileSourcesConfig) -> None:
        """Initialize file source pipeline."""
        self.config = config

    def run(
        self,
        products_df: pd.DataFrame,
        customers_df: pd.DataFrame,
    ) -> FileSourceOutputs:
        """
        Generate file-based source datasets.

        Args:
            products_df: Product catalog for update generation.
            customers_df: Customers for website event attribution.

        Returns:
            FileSourceOutputs with written file paths.
        """
        product_updates = ProductUpdateGenerator(self.config, products_df).generate()
        website_events = WebsiteEventGenerator(
            self.config, customers_df, products_df
        ).generate()

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        landing_dir = ensure_directory(self.config.landing_dir)

        pu_dir = ensure_directory(landing_dir / "product_updates")
        we_dir = ensure_directory(landing_dir / "website_events")

        pu_path = pu_dir / f"product_updates_{timestamp}.csv"
        we_path = we_dir / f"website_events_{timestamp}.json"

        product_updates.to_csv(pu_path, index=False, encoding="utf-8")
        with we_path.open("w", encoding="utf-8") as handle:
            json.dump(website_events, handle, indent=2, default=str)

        logger.info(
            "File sources written product_updates=%s website_events=%s",
            pu_path,
            we_path,
        )
        return FileSourceOutputs(
            product_updates_csv=pu_path,
            website_events_json=we_path,
            product_updates_rows=len(product_updates),
            website_events_rows=len(website_events),
        )
