# Architecture Documentation

> Retail Lakehouse Data Platform — architecture decisions and phase plan.

## 1. Executive Summary

The platform ingests retail transactional and event data, processes it through a **Medallion Architecture** on **Delta Lake**, loads curated datasets into **Snowflake**, and exposes analytics marts via **dbt** — orchestrated end-to-end by **Apache Airflow**.

**Phase 1** implements only the **source transactional layer** in PostgreSQL with synthetic data generation.

---

## 2. Phase 1 Architecture (Current)

### Components

| Component | Role |
|-----------|------|
| Python generators | Produce realistic customers, products, orders, items, payments |
| PostgreSQL 16 | OLTP-style source database with constraints and indexes |
| Docker Compose | Local PostgreSQL for development and demos |
| Validation scripts | Referential integrity and data-quality SQL checks |
| pytest | Unit tests for generators and pipeline orchestration |

### Data Flow

```
Faker + Pandas + NumPy
        │
        ▼
  Entity Generators ──► CSV files (data/generated/)
        │
        ▼
  PostgreSQL Loader ──► retail.* tables
        │
        ▼
  validate_phase1.py
```

### Key Schema Decisions

1. **Surrogate integer keys** — Simple joins; mirrors typical OLTP designs.
2. **CHECK constraints** — Enforce valid statuses, positive prices, and segment values at the database layer.
3. **Foreign keys** — Guarantee referential integrity before any downstream ingestion.
4. **Audit columns** — `created_at`, `updated_at`, `source_system` prepare for Bronze metadata mapping.
5. **Validation view** — `retail.v_table_counts` enables quick smoke tests.

---

## 3. Target Architecture (Phases 2–5)

### Phase 2 — Ingestion (Azure Data Factory) ✅

- Ingest PostgreSQL tables via ADF Copy activity (Parquet sink)
- Land product update **CSV** and website event **JSON** files in **Azure Data Lake Storage**
- Partition raw data by `ingestion_date` and `batch_id`
- Add ingestion metadata columns at copy time
- Local Python mirror for development without Azure credentials

See [docs/phase2-adf-ingestion.md](docs/phase2-adf-ingestion.md) for full details.

### Phase 4 — Silver (PySpark) ✅

- Validate Bronze data: nulls, accepted values, numeric bounds, duplicates
- Referential integrity checks with quarantine routing
- Deduplicate on primary key (latest `ingested_at` wins)
- Silver Delta MERGE + quarantine Delta append
- Local Spark pipeline mirroring Databricks execution

See [docs/phase4-silver-transforms.md](docs/phase4-silver-transforms.md).

### Phase 5 — Gold (Delta Lake) ✅

- Build conformed dimensions (`dim_date`, `dim_customers`, `dim_products`, `dim_country`)
- Build facts with revenue and payment flags (`fct_orders`, `fct_order_items`, `fct_payments`)
- Materialize business metric marts (daily sales, monthly revenue, CLV, product performance, segments)
- Delta MERGE for dimensions/facts; manifest per pipeline run
- Local Spark pipeline mirroring Databricks Gold jobs

See [docs/phase5-gold-models.md](docs/phase5-gold-models.md).

### Phase 6 — Warehouse Load (Snowflake) ✅

- Provision Snowflake database, schema, and role grants
- Load 12 Gold Delta tables into `RETAIL_DW.RAW`
- Auto-create tables from Gold schemas; overwrite (truncate + load) semantics
- Validate row counts against Gold sources
- Local Python pipeline mirroring production Snowflake loads

See [docs/phase6-snowflake-load.md](docs/phase6-snowflake-load.md).

### Phase 7 — dbt (Snowflake) ✅

- dbt project on Snowflake RAW tables:
  - `staging/` — source-conformed views over dims/facts
  - `intermediate/` — enriched joins and daily aggregates
  - `marts/` — 5 business metric tables in `RETAIL_DW.MARTS`
- dbt tests: `unique`, `not_null`, `relationships`, RAW reconciliation
- Schemas: `STAGING`, `INTERMEDIATE`, `MARTS`

See [docs/phase7-dbt-models.md](docs/phase7-dbt-models.md).

### Phase 8 — Orchestration (Airflow) ✅

- Apache Airflow DAGs orchestrating Phases 1→7
- `retail_platform_setup` — one-time Snowflake + dbt schema setup
- `retail_daily_pipeline` — scheduled full ETL with per-phase validation
- `retail_health_check` — weekly dry-run and compile checks
- Pandas reconciliation reports across Postgres, Bronze, and manifests
- Docker Compose Airflow services + GitHub Actions CI

See [docs/phase8-airflow-orchestration.md](docs/phase8-airflow-orchestration.md).

### Phase 9 — Testing, CI/CD & Documentation ✅

- Expanded pytest suite with Spark auto-markers, CLI smoke, loader, and DAG import tests
- GitHub Actions: lint (Ruff), typecheck (Mypy), Spark tests, coverage gate (60%)
- Dependabot, pre-commit hooks, PR template, CONTRIBUTING.md, MIT LICENSE
- Operator runbooks for local dev, pipeline failures, Airflow, and reconciliation

See [docs/phase9-testing-cicd-docs.md](docs/phase9-testing-cicd-docs.md).

---

## 4. Gold Layer Data Model (Phase 5) ✅

### Dimensions

- `dim_customers`
- `dim_products`
- `dim_date`
- `dim_country`

### Facts

- `fct_orders`
- `fct_order_items`
- `fct_payments`

### Marts

- `mart_daily_sales`
- `mart_monthly_revenue`
- `mart_customer_lifetime_value`
- `mart_product_performance`
- `mart_customer_segments`

---

## 5. Data Quality Framework (Planned)

| Check Type | Examples |
|------------|----------|
| Completeness | Null checks on required columns |
| Uniqueness | Duplicate primary keys |
| Referential integrity | Orphan foreign keys |
| Validity | Negative amounts, invalid statuses |
| Reconciliation | Source vs. target row counts and sums |

Invalid Silver records → **quarantine tables** with `rejection_reason`.

---

## 6. Security & Configuration

- All secrets in environment variables (`.env.example` only in repo)
- No hard-coded passwords, account names, or storage URLs
- Least-privilege IAM / service principals (Azure, Databricks, Snowflake) in later phases

---

## 7. Idempotency & Load Patterns

| Pattern | Phase | Use Case |
|---------|-------|----------|
| Truncate + full reload | 1 | Local dev reset |
| Append with batch ID | 2–3 | Bronze ingestion |
| MERGE / upsert | 3 | Silver and Gold dimensions |
| Incremental watermark | 3–4 | Fact table loads |
| dbt incremental | 4 | Mart refreshes |

---

## 8. Interview Talking Points

1. **Why Medallion?** Progressive refinement with auditable raw history.
2. **Why Delta Lake?** ACID transactions, time travel, and schema evolution on data lakes.
3. **Why dbt on Snowflake?** Separates transform logic from orchestration; testable SQL analytics engineering.
4. **Why Airflow?** Mature scheduler with observability for multi-system pipelines.
5. **Why PostgreSQL first?** Realistic CDC/copy-source pattern before cloud ingestion.

---

*Last updated: Phase 4 (Silver)*
