#!/usr/bin/env python3
"""Provision Snowflake database, schema, and grants for Gold loads."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.config.settings import (  # noqa: E402
    load_snowflake_config,
)
from retail_lakehouse.utils.logging import (  # noqa: E402
    configure_logging,
    get_logger,
)
from retail_lakehouse.warehouse.connector import (  # noqa: E402
    verify_connection,
)
from retail_lakehouse.warehouse.setup import SnowflakeSetup  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--sql-file",
    default=None,
    type=click.Path(path_type=Path),
    help="Optional setup SQL file (default: sql/snowflake/01_setup.sql)",
)
@click.option(
    "--skip-sql-file",
    is_flag=True,
    help="Only ensure database/schema context",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    sql_file: Path | None,
    skip_sql_file: bool,
    log_level: str,
) -> None:
    """
    Create Snowflake database objects required for Gold table loads.

    Example:
        python scripts/setup_snowflake.py
        python scripts/setup_snowflake.py --skip-sql-file
    """
    configure_logging(log_level)
    config = load_snowflake_config()

    click.echo("Verifying Snowflake connection...")
    version = verify_connection(config)
    click.echo(f"Connected to Snowflake version: {version}")

    setup = SnowflakeSetup(config)
    setup.ensure_context()
    click.echo(
        f"Ensured context: {config.database}.{config.schema} "
        f"warehouse={config.warehouse} role={config.role}"
    )

    if not skip_sql_file:
        path = sql_file or PROJECT_ROOT / "sql" / "snowflake" / "01_setup.sql"
        executed = setup.run(path)
        click.echo(f"Executed {len(executed)} setup statements from {path}")

    click.echo("\nSnowflake setup complete.")


if __name__ == "__main__":
    main()
