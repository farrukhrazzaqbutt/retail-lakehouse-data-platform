#!/usr/bin/env python3
"""Run dbt models against Snowflake."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"


def _run_dbt(command: list[str], env: dict[str, str]) -> int:
    """Execute a dbt CLI command in the project directory."""
    result = subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        env=env,
        check=False,
    )
    return result.returncode


def _dbt_env() -> dict[str, str]:
    """Build environment with dbt profile directory."""
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR / "profiles"))
    env.setdefault("DBT_TARGET", "dev")
    return env


@click.command()
@click.option(
    "--select",
    "select_arg",
    default=None,
    help="dbt --select argument (e.g. staging+, marts)",
)
@click.option("--full-refresh", is_flag=True, help="Pass --full-refresh to dbt run")
@click.option("--skip-deps", is_flag=True, help="Skip dbt deps")
@click.option("--deps-only", is_flag=True, help="Only run dbt deps")
@click.option(
    "--log-level", default="INFO", help="Logging level (unused, for CLI parity)"
)
def main(
    select_arg: str | None,
    full_refresh: bool,
    skip_deps: bool,
    deps_only: bool,
    log_level: str,
) -> None:
    """
    Run dbt staging, intermediate, and mart models.

    Example:
        python scripts/setup_dbt_schemas.py
        python scripts/run_dbt_models.py
        python scripts/run_dbt_models.py --select marts
        python scripts/run_dbt_models.py --select staging+
    """
    del log_level
    env = _dbt_env()

    if not skip_deps:
        click.echo("Installing dbt packages (dbt deps)...")
        code = _run_dbt(["dbt", "deps"], env)
        if code != 0:
            raise SystemExit(code)

    if deps_only:
        click.echo("dbt deps complete.")
        return

    command = ["dbt", "run"]
    if select_arg:
        command.extend(["--select", select_arg])
    if full_refresh:
        command.append("--full-refresh")

    click.echo(f"Running: {' '.join(command)}")
    code = _run_dbt(command, env)
    if code != 0:
        raise SystemExit(code)

    click.echo("\ndbt run complete.")


if __name__ == "__main__":
    main()
