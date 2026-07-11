"""PostgreSQL extraction utilities for ingestion pipelines."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from retail_lakehouse.config.settings import (
    AdfIngestionConfig,
    PostgresConfig,
    PostgresSourceTable,
)

logger = logging.getLogger(__name__)


class PostgresExtractor:
    """Extract PostgreSQL tables for landing in the raw data lake."""

    def __init__(
        self,
        postgres_config: PostgresConfig,
        ingestion_config: AdfIngestionConfig,
    ) -> None:
        """
        Initialize extractor.

        Args:
            postgres_config: Database connection settings.
            ingestion_config: Ingestion path and table definitions.
        """
        self.postgres_config = postgres_config
        self.ingestion_config = ingestion_config
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Lazy SQLAlchemy engine initialization."""
        if self._engine is None:
            if not self.postgres_config.password:
                raise ValueError("POSTGRES_PASSWORD is not set.")
            self._engine = create_engine(
                self.postgres_config.sqlalchemy_url,
                pool_pre_ping=True,
            )
        return self._engine

    def extract_table(
        self,
        table: PostgresSourceTable,
        watermark_value: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Extract a PostgreSQL table using full or incremental logic.

        Args:
            table: Table ingestion definition.
            watermark_value: Optional lower bound for incremental extracts.

        Returns:
            Extracted records as a DataFrame.
        """
        qualified = f"{self.ingestion_config.postgres_schema}.{table.entity}"
        query = f"SELECT * FROM {qualified}"
        params: dict[str, Any] = {}

        if (
            table.load_mode == "incremental"
            and table.watermark_column
            and watermark_value
        ):
            query += f" WHERE {table.watermark_column} > :watermark"
            params["watermark"] = watermark_value

        logger.info(
            "Extracting %s mode=%s rows_filter=%s",
            qualified,
            table.load_mode,
            "watermark" if params else "none",
        )
        with self.engine.connect() as connection:
            return pd.read_sql(text(query), connection, params=params or None)

    def get_row_count(self, entity: str) -> int:
        """Return current row count for a PostgreSQL table."""
        qualified = f"{self.ingestion_config.postgres_schema}.{entity}"
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
            return int(result.scalar_one())
