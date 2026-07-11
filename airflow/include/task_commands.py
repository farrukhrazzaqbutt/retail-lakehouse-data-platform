"""Shared Bash command builders for Airflow DAG tasks."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_PROJECT_ROOT = Path(
    os.getenv("RETAIL_PROJECT_ROOT", "/opt/retail-lakehouse")
)


def project_root() -> Path:
    """Return the mounted project root used by pipeline scripts."""
    return DEFAULT_PROJECT_ROOT


def pipeline_env() -> dict[str, str]:
    """
    Build environment variables for pipeline scripts inside Docker.

    Host-side overrides are preserved when already set in the environment.
    """
    defaults = {
        "RETAIL_PROJECT_ROOT": str(project_root()),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "retail_db"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "retail_user"),
        "POSTGRES_PASSWORD": os.getenv(
            "POSTGRES_PASSWORD", "change_me_secure_password"
        ),
        "ADLS_LOCAL_LANDING_DIR": os.getenv(
            "ADLS_LOCAL_LANDING_DIR", str(project_root() / "data/lakehouse/raw")
        ),
        "SILVER_BRONZE_ROOT": os.getenv(
            "SILVER_BRONZE_ROOT", str(project_root() / "data/lakehouse/raw")
        ),
        "SILVER_OUTPUT_ROOT": os.getenv(
            "SILVER_OUTPUT_ROOT", str(project_root() / "data/lakehouse/silver")
        ),
        "GOLD_OUTPUT_ROOT": os.getenv(
            "GOLD_OUTPUT_ROOT", str(project_root() / "data/lakehouse/gold")
        ),
        "SNOWFLAKE_MANIFEST_ROOT": os.getenv(
            "SNOWFLAKE_MANIFEST_ROOT",
            str(project_root() / "data/lakehouse/warehouse"),
        ),
        "DBT_PROFILES_DIR": os.getenv(
            "DBT_PROFILES_DIR", str(project_root() / "dbt/profiles")
        ),
        "DBT_TARGET": os.getenv("DBT_TARGET", "dev"),
        "JAVA_HOME": os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
        "PATH": os.getenv(
            "PATH",
            "/usr/lib/jvm/java-17-openjdk-amd64/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        ),
    }
    merged = dict(defaults)
    merged.update(os.environ)
    return merged


def script_command(script_name: str, *args: str) -> str:
    """
    Build a shell command that runs a project script from the repo root.

    Args:
        script_name: Script filename under scripts/.
        *args: CLI arguments appended to the command.

    Returns:
        Bash command string.
    """
    root = project_root()
    args_str = " ".join(args)
    return (
        f"cd {root} && "
        f"export PYTHONPATH={root / 'src'}:$PYTHONPATH && "
        f"python scripts/{script_name} {args_str}".strip()
    )
