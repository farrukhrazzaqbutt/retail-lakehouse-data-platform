"""Application settings loaded from environment variables and YAML config files."""

from __future__ import annotations

import os
from dataclasses import dataclass
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


@dataclass(frozen=True)
class FileSourcesConfig:
    """File-based source generation settings (CSV and JSON)."""

    data_generation: DataGenerationConfig
    product_updates_source_system: str
    product_updates_records: int
    product_update_types: list[WeightedOption]
    price_change_pct_range: tuple[float, float]
    website_events_source_system: str
    website_events_per_file: int
    event_types: list[WeightedOption]
    device_types: list[WeightedOption]
    channels: list[WeightedOption]
    landing_dir: Path


@dataclass(frozen=True)
class PostgresSourceTable:
    """PostgreSQL table ingestion definition."""

    entity: str
    load_mode: str
    watermark_column: str | None


@dataclass(frozen=True)
class FileSourceDefinition:
    """File source ingestion definition."""

    entity: str
    source_system: str
    source_type: str
    file_format: str
    staging_path: str
    pattern: str


@dataclass(frozen=True)
class AdfIngestionConfig:
    """Azure Data Factory ingestion configuration."""

    batch_id_prefix: str
    default_load_mode: str
    partition_column: str
    metadata_columns: list[str]
    adls_filesystem: str
    adls_base_path: str
    path_template: str
    postgres_source_system: str
    postgres_schema: str
    postgres_tables: list[PostgresSourceTable]
    file_sources: list[FileSourceDefinition]
    local_landing_dir: Path
    use_azurite: bool
    azurite_container: str


def load_file_sources_config(
    file_sources_path: Path | None = None,
    data_generation_path: Path | None = None,
) -> FileSourcesConfig:
    """Load file source generation settings from YAML."""
    path = file_sources_path or _resolve_path("config/file_sources.yaml")
    raw = _load_yaml(path)
    data_generation = load_data_generation_config(config_path=data_generation_path)

    def weighted_from_list(
        items: list[dict[str, Any]], key: str = "name"
    ) -> list[WeightedOption]:
        return [
            WeightedOption(name=str(item[key]), weight=float(item["weight"]))
            for item in items
        ]

    pu = raw["product_updates"]
    we = raw["website_events"]
    output = raw.get("output", {})

    return FileSourcesConfig(
        data_generation=data_generation,
        product_updates_source_system=str(pu["source_system"]),
        product_updates_records=int(pu["records_per_file"]),
        product_update_types=weighted_from_list(pu["update_types"], key="name"),
        price_change_pct_range=(
            float(pu["price_change_pct_range"][0]),
            float(pu["price_change_pct_range"][1]),
        ),
        website_events_source_system=str(we["source_system"]),
        website_events_per_file=int(we["events_per_file"]),
        event_types=weighted_from_list(we["event_types"], key="name"),
        device_types=weighted_from_list(we["devices"], key="name"),
        channels=weighted_from_list(we["channels"], key="name"),
        landing_dir=_resolve_path(output.get("landing_dir", "./data/file_sources")),
    )


def load_adf_ingestion_config(
    config_path: Path | None = None,
) -> AdfIngestionConfig:
    """Load ADF ingestion settings from YAML and environment variables."""
    load_dotenv()
    path = config_path or _resolve_path("config/adf_ingestion.yaml")
    raw = _load_yaml(path)

    ingestion = raw["ingestion"]
    adls = raw["adls"]
    pg = raw["postgres_sources"]
    local = raw.get("local_dev", {})

    postgres_tables = [
        PostgresSourceTable(
            entity=str(table["entity"]),
            load_mode=str(table.get("load_mode", "full")),
            watermark_column=table.get("watermark_column"),
        )
        for table in pg["tables"]
    ]

    file_sources = [
        FileSourceDefinition(
            entity=str(source["entity"]),
            source_system=str(source["source_system"]),
            source_type=str(source["source_type"]),
            file_format=str(source["format"]),
            staging_path=str(source["staging_path"]),
            pattern=str(source["pattern"]),
        )
        for source in raw["file_sources"]
    ]

    return AdfIngestionConfig(
        batch_id_prefix=str(ingestion.get("batch_id_prefix", "retail")),
        default_load_mode=str(ingestion.get("default_load_mode", "full")),
        partition_column=str(ingestion.get("partition_column", "ingestion_date")),
        metadata_columns=list(ingestion.get("metadata_columns", [])),
        adls_filesystem=os.getenv(
            "AZURE_STORAGE_CONTAINER", adls.get("filesystem", "raw")
        ),
        adls_base_path=adls.get("base_path", "bronze"),
        path_template=str(
            adls.get(
                "path_template",
                "{base_path}/{source_type}/{entity}/ingestion_date={ingestion_date}/batch_id={batch_id}",
            )
        ),
        postgres_source_system=str(pg.get("source_system", "retail_postgres")),
        postgres_schema=str(pg.get("schema", "retail")),
        postgres_tables=postgres_tables,
        file_sources=file_sources,
        local_landing_dir=_resolve_path(
            os.getenv(
                "ADLS_LOCAL_LANDING_DIR",
                local.get("landing_dir", "./data/lakehouse/raw"),
            )
        ),
        use_azurite=os.getenv(
            "ADLS_USE_AZURITE", str(local.get("use_azurite", False))
        ).lower()
        == "true",
        azurite_container=os.getenv(
            "AZURITE_CONTAINER", local.get("azurite_container", "raw")
        ),
    )


@dataclass(frozen=True)
class NumericConstraint:
    """Numeric column bounds for Silver validation."""

    min_inclusive: float | None = None
    max_inclusive: float | None = None
    min_exclusive: float | None = None
    max_exclusive: float | None = None


@dataclass(frozen=True)
class SilverEntityConfig:
    """Silver transformation rules for a single entity."""

    name: str
    source_type: str
    file_format: str
    primary_key: str
    required_columns: list[str]
    accepted_values: dict[str, list[str]]
    numeric_constraints: dict[str, NumericConstraint]
    foreign_keys: dict[str, str]
    dedupe_keys: list[str]


@dataclass(frozen=True)
class ReferentialIntegrityCheck:
    """Cross-entity referential integrity rule."""

    child_entity: str
    child_column: str
    parent_entity: str
    parent_column: str


@dataclass(frozen=True)
class SilverTransformConfig:
    """Silver layer transformation configuration."""

    bronze_base_path: str
    silver_base_path: str
    quarantine_base_path: str
    processed_at_column: str
    rejection_reason_column: str
    bronze_metadata_columns: list[str]
    dedupe_order_columns: list[str]
    entities: list[SilverEntityConfig]
    referential_integrity_enabled: bool
    referential_integrity_checks: list[ReferentialIntegrityCheck]
    bronze_root: Path
    silver_root: Path


def load_silver_transform_config(
    config_path: Path | None = None,
) -> SilverTransformConfig:
    """Load Silver transformation settings from YAML and environment variables."""
    load_dotenv()
    path = config_path or _resolve_path("config/silver_transforms.yaml")
    raw = _load_yaml(path)

    silver = raw["silver"]
    dedupe = raw.get("deduplication", {})
    local = raw.get("local_dev", {})
    ri = raw.get("referential_integrity", {})

    entities: list[SilverEntityConfig] = []
    for name, cfg in raw["entities"].items():
        numeric_constraints: dict[str, NumericConstraint] = {}
        for column, bounds in cfg.get("numeric_constraints", {}).items():
            numeric_constraints[column] = NumericConstraint(
                min_inclusive=bounds.get("min_inclusive"),
                max_inclusive=bounds.get("max_inclusive"),
                min_exclusive=bounds.get("min_exclusive"),
                max_exclusive=bounds.get("max_exclusive"),
            )

        entities.append(
            SilverEntityConfig(
                name=name,
                source_type=str(cfg["source_type"]),
                file_format=str(cfg["format"]),
                primary_key=str(cfg["primary_key"]),
                required_columns=list(cfg.get("required_columns", [])),
                accepted_values={
                    col: list(values)
                    for col, values in cfg.get("accepted_values", {}).items()
                },
                numeric_constraints=numeric_constraints,
                foreign_keys={
                    col: str(ref) for col, ref in cfg.get("foreign_keys", {}).items()
                },
                dedupe_keys=list(cfg.get("dedupe_keys", [cfg["primary_key"]])),
            )
        )

    referential_checks = [
        ReferentialIntegrityCheck(
            child_entity=str(check["child_entity"]),
            child_column=str(check["child_column"]),
            parent_entity=str(check["parent_entity"]),
            parent_column=str(check["parent_column"]),
        )
        for check in ri.get("checks", [])
    ]

    return SilverTransformConfig(
        bronze_base_path=str(silver.get("bronze_base_path", "bronze")),
        silver_base_path=str(silver.get("silver_base_path", "silver")),
        quarantine_base_path=str(
            silver.get("quarantine_base_path", "silver/_quarantine")
        ),
        processed_at_column=str(silver.get("processed_at_column", "processed_at")),
        rejection_reason_column=str(
            silver.get("rejection_reason_column", "rejection_reason")
        ),
        bronze_metadata_columns=list(silver.get("bronze_metadata_columns", [])),
        dedupe_order_columns=list(
            dedupe.get("default_order_columns", ["ingested_at", "batch_id"])
        ),
        entities=entities,
        referential_integrity_enabled=bool(ri.get("enabled", True)),
        referential_integrity_checks=referential_checks,
        bronze_root=_resolve_path(
            os.getenv(
                "SILVER_BRONZE_ROOT", local.get("bronze_root", "./data/lakehouse/raw")
            )
        ),
        silver_root=_resolve_path(
            os.getenv(
                "SILVER_OUTPUT_ROOT",
                local.get("silver_root", "./data/lakehouse/silver"),
            )
        ),
    )


@dataclass(frozen=True)
class GoldModelConfig:
    """Gold layer model configuration."""

    gold_base_path: str
    silver_base_path: str
    processed_at_column: str
    dimensions: list[str]
    facts: list[str]
    marts: list[str]
    date_start: str
    date_end: str
    completed_statuses: list[str]
    cancelled_statuses: list[str]
    refunded_statuses: list[str]
    successful_payment_statuses: list[str]
    failed_payment_statuses: list[str]
    silver_root: Path
    gold_root: Path


def load_gold_model_config(
    config_path: Path | None = None,
) -> GoldModelConfig:
    """Load Gold model settings from YAML and environment variables."""
    load_dotenv()
    path = config_path or _resolve_path("config/gold_models.yaml")
    raw = _load_yaml(path)

    gold = raw["gold"]
    date_dim = raw.get("date_dimension", {})
    rules = raw.get("business_rules", {})
    local = raw.get("local_dev", {})

    return GoldModelConfig(
        gold_base_path=str(gold.get("gold_base_path", "gold")),
        silver_base_path=str(gold.get("silver_base_path", "silver")),
        processed_at_column=str(gold.get("processed_at_column", "gold_loaded_at")),
        dimensions=list(gold.get("dimensions", [])),
        facts=list(gold.get("facts", [])),
        marts=list(gold.get("marts", [])),
        date_start=str(date_dim.get("start_date", "2023-01-01")),
        date_end=str(date_dim.get("end_date", "2026-12-31")),
        completed_statuses=list(rules.get("completed_statuses", ["completed"])),
        cancelled_statuses=list(rules.get("cancelled_statuses", ["cancelled"])),
        refunded_statuses=list(rules.get("refunded_statuses", ["refunded"])),
        successful_payment_statuses=list(
            rules.get("successful_payment_statuses", ["succeeded"])
        ),
        failed_payment_statuses=list(rules.get("failed_payment_statuses", ["failed"])),
        silver_root=_resolve_path(
            os.getenv(
                "SILVER_OUTPUT_ROOT",
                local.get("silver_root", "./data/lakehouse/silver"),
            )
        ),
        gold_root=_resolve_path(
            os.getenv(
                "GOLD_OUTPUT_ROOT", local.get("gold_root", "./data/lakehouse/gold")
            )
        ),
    )


@dataclass(frozen=True)
class SnowflakeConfig:
    """Snowflake connection settings."""

    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


@dataclass(frozen=True)
class SnowflakeLoadLayer:
    """Ordered Snowflake load group for a Gold layer."""

    layer: str
    tables: list[str]


@dataclass(frozen=True)
class SnowflakeLoadConfig:
    """Snowflake Gold load configuration."""

    gold_base_path: str
    load_mode: str
    manifest_base_path: str
    auto_create_tables: bool
    uppercase_identifiers: bool
    load_order: list[SnowflakeLoadLayer]
    gold_root: Path
    manifest_root: Path


def load_snowflake_config(env_file: Path | None = None) -> SnowflakeConfig:
    """Load Snowflake connection settings from environment variables."""
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()

    return SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        role=os.getenv("SNOWFLAKE_ROLE", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        database=os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DW"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )


def load_snowflake_load_config(
    config_path: Path | None = None,
) -> SnowflakeLoadConfig:
    """Load Snowflake Gold load settings from YAML and environment."""
    load_dotenv()
    path = config_path or _resolve_path("config/snowflake_load.yaml")
    raw = _load_yaml(path)

    load_cfg = raw["snowflake_load"]
    local = raw.get("local_dev", {})

    load_order = [
        SnowflakeLoadLayer(
            layer=str(group["layer"]),
            tables=[str(table) for table in group["tables"]],
        )
        for group in load_cfg.get("load_order", [])
    ]

    return SnowflakeLoadConfig(
        gold_base_path=str(load_cfg.get("gold_base_path", "gold")),
        load_mode=str(load_cfg.get("load_mode", "overwrite")),
        manifest_base_path=str(
            load_cfg.get("manifest_base_path", "warehouse/_manifests")
        ),
        auto_create_tables=bool(load_cfg.get("auto_create_tables", True)),
        uppercase_identifiers=bool(load_cfg.get("uppercase_identifiers", True)),
        load_order=load_order,
        gold_root=_resolve_path(
            os.getenv(
                "GOLD_OUTPUT_ROOT", local.get("gold_root", "./data/lakehouse/gold")
            )
        ),
        manifest_root=_resolve_path(
            os.getenv(
                "SNOWFLAKE_MANIFEST_ROOT",
                local.get("manifest_root", "./data/lakehouse/warehouse"),
            )
        ),
    )


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
            WeightedOption(
                name=item["name"]
                if "name" in item
                else item["status"]
                if "status" in item
                else item["method"],
                weight=float(item["weight"]),
            )
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
        CountryConfig(
            code=item["code"], name=item["name"], weight=float(item["weight"])
        )
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
