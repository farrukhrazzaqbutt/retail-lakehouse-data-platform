"""Load generated DataFrames into PostgreSQL with idempotent upsert semantics."""

from __future__ import annotations

import logging
from collections.abc import Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from retail_lakehouse.config.settings import PostgresConfig, PostgresTableConfig

logger = logging.getLogger(__name__)

TABLE_LOAD_ORDER: list[str] = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


class PostgresLoader:
    """Load tabular datasets into PostgreSQL respecting foreign-key order."""

    def __init__(
        self,
        postgres_config: PostgresConfig,
        table_config: PostgresTableConfig,
    ) -> None:
        """
        Initialize loader with connection and table metadata.

        Args:
            postgres_config: Database connection settings.
            table_config: Table names and load behavior.
        """
        self.postgres_config = postgres_config
        self.table_config = table_config
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Lazy SQLAlchemy engine initialization."""
        if self._engine is None:
            if not self.postgres_config.password:
                raise ValueError(
                    "POSTGRES_PASSWORD is not set. Copy .env.example to .env and configure it."
                )
            self._engine = create_engine(
                self.postgres_config.sqlalchemy_url,
                pool_pre_ping=True,
            )
        return self._engine

    def verify_connection(self) -> None:
        """Validate database connectivity."""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise ConnectionError(
                "Unable to connect to PostgreSQL. Ensure Docker Compose is running."
            ) from exc

    def truncate_tables(self, tables: Iterable[str] | None = None) -> None:
        """
        Truncate target tables in reverse dependency order.

        Args:
            tables: Optional subset of tables to truncate.
        """
        target_tables = list(tables) if tables else list(reversed(TABLE_LOAD_ORDER))
        qualified_tables = [
            f"{self.table_config.schema}.{self.table_config.tables[name]['name']}"
            for name in target_tables
        ]
        statement = (
            f"TRUNCATE TABLE {', '.join(qualified_tables)} RESTART IDENTITY CASCADE;"
        )
        logger.info("Truncating tables: %s", ", ".join(target_tables))
        with self.engine.begin() as connection:
            connection.execute(text(statement))

    def load_table(self, table_key: str, df: pd.DataFrame) -> int:
        """
        Append rows to a PostgreSQL table using batched inserts.

        Args:
            table_key: Logical table key from configuration.
            df: DataFrame to load.

        Returns:
            Number of rows written.
        """
        if df.empty:
            logger.warning("Skipping empty dataset for table '%s'", table_key)
            return 0

        table_name = self.table_config.tables[table_key]["name"]
        qualified_name = f"{self.table_config.schema}.{table_name}"
        logger.info("Loading %s rows into %s", len(df), qualified_name)

        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.table_config.schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.table_config.batch_size,
            )
        except SQLAlchemyError as exc:
            raise RuntimeError(f"Failed to load table '{table_key}': {exc}") from exc

        return len(df)

    def load_all(
        self, datasets: dict[str, pd.DataFrame], truncate_first: bool = False
    ) -> dict[str, int]:
        """
        Load all datasets in foreign-key-safe order.

        Args:
            datasets: Mapping of table key to DataFrame.
            truncate_first: Whether to truncate tables before loading.

        Returns:
            Mapping of table key to rows loaded.
        """
        self.verify_connection()
        if truncate_first:
            self.truncate_tables()

        results: dict[str, int] = {}
        for table_key in TABLE_LOAD_ORDER:
            if table_key not in datasets:
                raise KeyError(f"Missing dataset for table '{table_key}'")
            results[table_key] = self.load_table(table_key, datasets[table_key])
        return results

    def get_row_counts(self) -> pd.DataFrame:
        """Return row counts from the validation view."""
        query = text(
            f"SELECT * FROM {self.table_config.schema}.v_table_counts ORDER BY table_name"
        )
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)


def load_dataframes_to_postgres(
    datasets: dict[str, pd.DataFrame],
    postgres_config: PostgresConfig,
    table_config: PostgresTableConfig,
    truncate_first: bool = False,
) -> dict[str, int]:
    """
    Convenience function to load datasets into PostgreSQL.

    Args:
        datasets: Entity DataFrames keyed by logical table name.
        postgres_config: Database connection settings.
        table_config: Table metadata and load options.
        truncate_first: Whether to truncate before append.

    Returns:
        Rows loaded per table.
    """
    loader = PostgresLoader(postgres_config, table_config)
    return loader.load_all(datasets, truncate_first=truncate_first)
