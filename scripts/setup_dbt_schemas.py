#!/usr/bin/env python3
"""Provision Snowflake schemas for dbt staging, intermediate, and marts."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import load_snowflake_config  # noqa: E402
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402
from retail_lakehouse.warehouse.connector import (  # noqa: E402
    get_connection,
    verify_connection,
)
from retail_lakehouse.warehouse.setup import SnowflakeSetup  # noqa: E402

logger = get_logger(__name__)

DEFAULT_SQL = PROJECT_ROOT / "sql" / "snowflake" / "02_dbt_schemas.sql"


def _render_sql(config, sql_path: Path) -> list[str]:
    """Load and render dbt schema setup SQL."""
    raw = sql_path.read_text(encoding="utf-8")
    replacements = {
        "{{DATABASE}}": config.database,
        "{{SCHEMA_STAGING}}": "STAGING",
        "{{SCHEMA_INTERMEDIATE}}": "INTERMEDIATE",
        "{{SCHEMA_MARTS}}": "MARTS",
        "{{ROLE}}": config.role,
        "{{WAREHOUSE}}": config.warehouse,
    }
    for token, value in replacements.items():
        raw = raw.replace(token, value)

    statements: list[str] = []
    buffer: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer).rstrip(";").strip())
            buffer = []
    if buffer:
        statements.append("\n".join(buffer).strip())
    return statements


@click.command()
@click.option(
    "--sql-file",
    default=None,
    type=click.Path(path_type=Path),
    help="Optional SQL file (default: sql/snowflake/02_dbt_schemas.sql)",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(sql_file: Path | None, log_level: str) -> None:
    """Create STAGING, INTERMEDIATE, and MARTS schemas in Snowflake."""
    configure_logging(log_level)
    config = load_snowflake_config()
    path = sql_file or DEFAULT_SQL

    click.echo("Verifying Snowflake connection...")
    version = verify_connection(config)
    click.echo(f"Connected to Snowflake version: {version}")

    setup = SnowflakeSetup(config)
    setup.ensure_context()

    statements = _render_sql(config, path)
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
            click.echo(f"  Executed: {statement.splitlines()[0][:100]}")
    finally:
        connection.close()

    click.echo(
        f"\nCreated dbt schemas in {config.database}: STAGING, INTERMEDIATE, MARTS"
    )


if __name__ == "__main__":
    main()
