"""Application settings loaded from environment variables and YAML config files."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from urllib.parse import quote_plus

import yaml
from dotenv import load_dotenv

DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _resolve_path(path: str | Path, base: Path = DEFAULT_PROJECT_ROOT) -> Path:
    """Resolve a path relative to the project root when not absolute."""
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = base / resolved
    return resolved.resolve()


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML configuration file."""
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in config file: {path}")
    return data


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL connection settings."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "retail"

    @property
    def sqlalchemy_url(self) -> str:
        """Return SQLAlchemy connection URL."""
        password = quote_plus(self.password)
        return (
            f"postgresql+psycopg2://{self.user}:{password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class WeightedOption:
    """A weighted categorical option used in synthetic data generation."""

    name: str
    weight: float


@dataclass(frozen=True)
class ProductCategoryConfig:
    """Product category definition with subcategories and price bounds."""

    category: str
    subcategories: list[str]
    price_range: tuple[float, float]
    weight: float


@dataclass(frozen=True)
class CountryConfig:
    """Country definition for customer and order geography."""

    code: str
    name: str
    weight: float


@dataclass(frozen=True)
class DataGenerationConfig:
    """Synthetic data generation parameters."""

    seed: int
    num_customers: int
    num_products: int
    num_orders: int
    order_start_date: date
    order_end_date: date
    customer_segments: list[WeightedOption]
    order_statuses: list[WeightedOption]
    payment_methods: list[WeightedOption]
    payment_statuses: list[WeightedOption]
    product_categories: list[ProductCategoryConfig]
    countries: list[CountryConfig]
    min_items_per_order: int
    max_items_per_order: int
    discount_probability: float
    max_discount_pct: float
    output_dir: Path
    sample_output_dir: Path
    source_system: str = "retail_postgres_generator"


@dataclass(frozen=True)
class PostgresTableConfig:
    """PostgreSQL table metadata for loading."""

    schema: str
    tables: dict[str, dict[str, Any]]
    batch_size: int
    truncate_before_load: bool
    source_system: str


def load_postgres_config(env_file: Path | None = None) -> PostgresConfig:
    """
    Load PostgreSQL settings from environment variables.

    Args:
        env_file: Optional path to a `.env` file.

    Returns:
        PostgresConfig instance.
    """
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()

    return PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "55432")),
        database=os.getenv("POSTGRES_DB", "retail_db"),
        user=os.getenv("POSTGRES_USER", "retail_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        schema=os.getenv("POSTGRES_SCHEMA", "retail"),
    )


def load_data_generation_config(
    config_path: Path | None = None,
    env_file: Path | None = None,
) -> DataGenerationConfig:
    """
    Load data generation settings from YAML and environment variables.

    Environment variables override YAML defaults for volume and date range.
    """
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()

    path = config_path or _resolve_path("config/data_generation.yaml")
    raw = _load_yaml(path)

    def weighted_options(key: str) -> list[WeightedOption]:
        return [
            WeightedOption(name=item["name"] if "name" in item else item["status"] if "status" in item else item["method"], weight=float(item["weight"]))
            for item in raw[key]
        ]

    payment_methods = [
        WeightedOption(name=item["method"], weight=float(item["weight"]))
        for item in raw["payment_methods"]
    ]
    payment_statuses = [
        WeightedOption(name=item["status"], weight=float(item["weight"]))
        for item in raw["payment_statuses"]
    ]
    order_statuses = [
        WeightedOption(name=item["status"], weight=float(item["weight"]))
        for item in raw["order_statuses"]
    ]

    product_categories = [
        ProductCategoryConfig(
            category=item["category"],
            subcategories=list(item["subcategories"]),
            price_range=(float(item["price_range"][0]), float(item["price_range"][1])),
            weight=float(item["weight"]),
        )
        for item in raw["product_categories"]
    ]

    countries = [
        CountryConfig(code=item["code"], name=item["name"], weight=float(item["weight"]))
        for item in raw["countries"]
    ]

    return DataGenerationConfig(
        seed=int(os.getenv("DATA_GENERATION_SEED", raw.get("seed", 42))),
        num_customers=int(os.getenv("NUM_CUSTOMERS", raw["volumes"]["customers"])),
        num_products=int(os.getenv("NUM_PRODUCTS", raw["volumes"]["products"])),
        num_orders=int(os.getenv("NUM_ORDERS", raw["volumes"]["orders"])),
        order_start_date=date.fromisoformat(
            os.getenv("ORDER_START_DATE", raw["date_range"]["start"])
        ),
        order_end_date=date.fromisoformat(
            os.getenv("ORDER_END_DATE", raw["date_range"]["end"])
        ),
        customer_segments=weighted_options("customer_segments"),
        order_statuses=order_statuses,
        payment_methods=payment_methods,
        payment_statuses=payment_statuses,
        product_categories=product_categories,
        countries=countries,
        min_items_per_order=int(raw["order_items"]["min_per_order"]),
        max_items_per_order=int(raw["order_items"]["max_per_order"]),
        discount_probability=float(raw["order_items"]["discount_probability"]),
        max_discount_pct=float(raw["order_items"]["max_discount_pct"]),
        output_dir=_resolve_path(os.getenv("DATA_OUTPUT_DIR", "./data/generated")),
        sample_output_dir=_resolve_path(
            os.getenv("DATA_SAMPLE_OUTPUT_DIR", "./data/samples")
        ),
    )


def load_postgres_table_config(
    config_path: Path | None = None,
) -> PostgresTableConfig:
    """Load PostgreSQL table metadata from YAML."""
    path = config_path or _resolve_path("config/postgres_tables.yaml")
    raw = _load_yaml(path)
    load_cfg = raw.get("load", {})
    return PostgresTableConfig(
        schema=raw.get("schema", "retail"),
        tables=raw["tables"],
        batch_size=int(load_cfg.get("batch_size", 1000)),
        truncate_before_load=bool(load_cfg.get("truncate_before_load", False)),
        source_system=str(load_cfg.get("source_system", "retail_postgres_generator")),
    )
