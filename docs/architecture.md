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

### Phase 2 — Ingestion (Azure Data Factory)

- Ingest PostgreSQL tables via copy activity
- Land product update **CSV** and website event **JSON** files in **Azure Data Lake Storage**
- Partition raw data by `ingestion_date` and `source_system`
- Add pipeline parameters from configuration (no hard-coded storage paths)

### Phase 3 — Processing (Databricks + Delta Lake)

**Medallion layers:**

| Layer | Purpose |
|-------|---------|
| Bronze | Raw payloads + ingestion metadata (`batch_id`, `source_file`, `ingested_at`) |
| Silver | Cleaned, deduplicated, validated; quarantine tables for rejected records |
| Gold | `dim_*` and `fct_*` tables plus business aggregates |

**Delta Lake features to demonstrate:**

- MERGE for upserts and slowly changing products
- Schema enforcement and schema evolution
- `DESCRIBE HISTORY` and time travel
- Incremental processing with watermark columns

### Phase 4 — Warehouse (Snowflake + dbt)

- Load Gold tables into Snowflake raw schema
- dbt project structure:
  - `staging/` — source-conformed models
  - `intermediate/` — business logic joins
  - `marts/` — `mart_daily_sales`, `mart_monthly_revenue`, etc.
- dbt tests (unique, not_null, relationships, accepted_values)
- Seeds, snapshots, macros, incremental models, and documentation

### Phase 5 — Orchestration & CI/CD

- **Apache Airflow** DAGs for ingestion → Databricks → Snowflake → dbt → reconciliation
- **Pandas** source-to-target reconciliation reports
- **Docker Compose** extended with Airflow services
- **GitHub Actions** for linting, pytest, PySpark tests, and `dbt compile`
- Full architecture diagram and runbook

---

## 4. Gold Layer Data Model (Planned)

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

*Last updated: Phase 1*
