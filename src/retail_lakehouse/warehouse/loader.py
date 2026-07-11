"""Load Gold DataFrames into Snowflake tables."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame

from retail_lakehouse.config.settings import SnowflakeConfig, SnowflakeLoadConfig
from retail_lakehouse.warehouse.connector import get_connection
from retail_lakehouse.warehouse.ddl import (
    generate_create_table_sql,
    normalize_identifier,
)

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Write Gold tables into Snowflake RAW schema."""

    def __init__(
        self,
        snowflake_config: SnowflakeConfig,
        load_config: SnowflakeLoadConfig,
    ) -> None:
        """
        Initialize Snowflake loader.

        Args:
            snowflake_config: Snowflake connection settings.
            load_config: Gold load behavior configuration.
        """
        self.snowflake_config = snowflake_config
        self.load_config = load_config

    def ensure_table(self, table_name: str, df: DataFrame) -> None:
        """
        Create a Snowflake table when auto-create is enabled.

        Args:
            table_name: Target table name.
            df: Source DataFrame used to infer schema.
        """
        if not self.load_config.auto_create_tables:
            return

        ddl = generate_create_table_sql(
            table_name=table_name,
            df=df,
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema,
            uppercase_identifiers=self.load_config.uppercase_identifiers,
        )
        connection = get_connection(self.snowflake_config)
        try:
            cursor = connection.cursor()
            cursor.execute(ddl)
            logger.info("Ensured Snowflake table=%s", table_name)
        finally:
            connection.close()

    def load_dataframe(self, table_name: str, df: DataFrame) -> int:
        """
        Load a Spark DataFrame into Snowflake.

        Args:
            table_name: Target Snowflake table name.
            df: Source DataFrame.

        Returns:
            Number of rows written.
        """
        pandas_df = df.toPandas()
        return self.load_pandas(table_name, pandas_df)

    def load_pandas(self, table_name: str, df: pd.DataFrame) -> int:
        """
        Load a pandas DataFrame into Snowflake.

        Args:
            table_name: Target Snowflake table name.
            df: Source DataFrame.

        Returns:
            Number of rows written.
        """
        if df.empty:
            logger.info("Skipping empty Snowflake load table=%s", table_name)
            return 0

        from snowflake.connector.pandas_tools import write_pandas

        target_table = normalize_identifier(
            table_name,
            self.load_config.uppercase_identifiers,
        )
        database = normalize_identifier(
            self.snowflake_config.database,
            self.load_config.uppercase_identifiers,
        )
        schema = normalize_identifier(
            self.snowflake_config.schema,
            self.load_config.uppercase_identifiers,
        )

        connection = get_connection(self.snowflake_config)
        try:
            if self.load_config.load_mode == "overwrite":
                self._truncate_table(connection, database, schema, target_table)

            _, _, row_count, _ = write_pandas(
                connection,
                df,
                table_name=target_table,
                database=database,
                schema=schema,
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
            )
            logger.info(
                "Snowflake load complete table=%s rows=%s mode=%s",
                target_table,
                row_count,
                self.load_config.load_mode,
            )
            return int(row_count)
        finally:
            connection.close()

    def table_row_count(self, table_name: str) -> int:
        """
        Return the current row count for a Snowflake table.

        Args:
            table_name: Target table name.

        Returns:
            Row count.
        """
        target_table = normalize_identifier(
            table_name,
            self.load_config.uppercase_identifiers,
        )
        database = normalize_identifier(
            self.snowflake_config.database,
            self.load_config.uppercase_identifiers,
        )
        schema = normalize_identifier(
            self.snowflake_config.schema,
            self.load_config.uppercase_identifiers,
        )
        sql = f"SELECT COUNT(*) FROM {database}.{schema}.{target_table}"

        connection = get_connection(self.snowflake_config)
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            return int(cursor.fetchone()[0])
        finally:
            connection.close()

    def table_exists(self, table_name: str) -> bool:
        """Return True when a Snowflake table exists in the target schema."""
        target_table = table_name.upper()
        sql = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = %s
              AND TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
        """
        connection = get_connection(self.snowflake_config)
        try:
            cursor = connection.cursor()
            cursor.execute(
                sql,
                (
                    self.snowflake_config.database.upper(),
                    self.snowflake_config.schema.upper(),
                    target_table,
                ),
            )
            return int(cursor.fetchone()[0]) > 0
        finally:
            connection.close()

    @staticmethod
    def _truncate_table(
        connection: Any,
        database: str,
        schema: str,
        table_name: str,
    ) -> None:
        """Truncate a Snowflake table before overwrite loads."""
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {database}.{schema}.{table_name}")
