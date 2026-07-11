"""Snowflake connection management."""

from __future__ import annotations

import logging
from typing import Any

from retail_lakehouse.config.settings import SnowflakeConfig

logger = logging.getLogger(__name__)

REQUIRED_CONNECTION_FIELDS = (
    "account",
    "user",
    "password",
    "role",
    "warehouse",
    "database",
    "schema",
)


class SnowflakeConnectionError(ConnectionError):
    """Raised when Snowflake connection or credentials are invalid."""


def validate_snowflake_config(config: SnowflakeConfig) -> None:
    """
    Validate required Snowflake connection fields are set.

    Args:
        config: Snowflake connection configuration.

    Raises:
        SnowflakeConnectionError: If required fields are missing.
    """
    missing = [
        field for field in REQUIRED_CONNECTION_FIELDS if not getattr(config, field)
    ]
    if missing:
        raise SnowflakeConnectionError(
            "Missing Snowflake credentials: "
            f"{', '.join(missing)}. "
            "Copy .env.example to .env and configure SNOWFLAKE_* variables."
        )


def get_connection(config: SnowflakeConfig) -> Any:
    """
    Create a Snowflake connector connection.

    Args:
        config: Snowflake connection configuration.

    Returns:
        snowflake.connector.Connection instance.
    """
    validate_snowflake_config(config)

    import snowflake.connector

    logger.info(
        "Connecting to Snowflake account=%s database=%s schema=%s",
        config.account,
        config.database,
        config.schema,
    )
    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
    )


def verify_connection(config: SnowflakeConfig) -> str:
    """
    Verify Snowflake connectivity and return current context.

    Args:
        config: Snowflake connection configuration.

    Returns:
        Snowflake version string.
    """
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        cursor.execute(
            "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"
        )
        context = cursor.fetchone()
        logger.info(
            "Snowflake connected version=%s context=%s",
            version,
            context,
        )
        return str(version)
    finally:
        connection.close()


def execute_sql(config: SnowflakeConfig, sql: str) -> None:
    """
    Execute a SQL statement against Snowflake.

    Args:
        config: Snowflake connection configuration.
        sql: SQL statement to execute.
    """
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
    finally:
        connection.close()
