"""Snowflake database and schema setup."""

from __future__ import annotations

import logging
from pathlib import Path

from retail_lakehouse.config.settings import SnowflakeConfig
from retail_lakehouse.warehouse.connector import execute_sql, get_connection

logger = logging.getLogger(__name__)

DEFAULT_SETUP_SQL = (
    Path(__file__).resolve().parents[3] / "sql" / "snowflake" / "01_setup.sql"
)


class SnowflakeSetup:
    """Provision Snowflake database objects for the retail warehouse."""

    def __init__(self, config: SnowflakeConfig) -> None:
        """
        Initialize Snowflake setup helper.

        Args:
            config: Snowflake connection configuration.
        """
        self.config = config

    def run(self, sql_path: Path | None = None) -> list[str]:
        """
        Execute setup SQL statements.

        Args:
            sql_path: Optional path to a setup SQL file.

        Returns:
            List of executed statement summaries.
        """
        path = sql_path or DEFAULT_SETUP_SQL
        statements = self._load_statements(path)
        executed: list[str] = []

        connection = get_connection(self.config)
        try:
            cursor = connection.cursor()
            for statement in statements:
                rendered = self._render_statement(statement)
                cursor.execute(rendered)
                summary = rendered.splitlines()[0][:120]
                executed.append(summary)
                logger.info("Executed setup SQL: %s", summary)
        finally:
            connection.close()

        return executed

    def _render_statement(self, statement: str) -> str:
        """Replace setup placeholders with configured values."""
        replacements = {
            "{{DATABASE}}": self.config.database,
            "{{SCHEMA}}": self.config.schema,
            "{{WAREHOUSE}}": self.config.warehouse,
            "{{ROLE}}": self.config.role,
        }
        rendered = statement
        for token, value in replacements.items():
            rendered = rendered.replace(token, value)
        return rendered

    @staticmethod
    def _load_statements(path: Path) -> list[str]:
        """Load semicolon-delimited SQL statements from a setup file."""
        if not path.exists():
            raise FileNotFoundError(f"Snowflake setup SQL not found: {path}")

        raw = path.read_text(encoding="utf-8")
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

    def ensure_context(self) -> None:
        """Ensure database, schema, warehouse, and role are usable."""
        execute_sql(
            self.config,
            f"USE ROLE {self.config.role}",
        )
        execute_sql(
            self.config,
            f"USE WAREHOUSE {self.config.warehouse}",
        )
        execute_sql(
            self.config,
            f"CREATE DATABASE IF NOT EXISTS {self.config.database}",
        )
        execute_sql(
            self.config,
            f"CREATE SCHEMA IF NOT EXISTS {self.config.database}.{self.config.schema}",
        )
        execute_sql(
            self.config,
            f"USE SCHEMA {self.config.database}.{self.config.schema}",
        )
