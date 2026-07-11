# Runbook — Pipeline Failures

Per-phase recovery guide for the retail lakehouse pipeline.

## Phase 1 — PostgreSQL

**Symptoms:** `validate_phase1.py` reports orphan FKs or negative amounts.

**Recovery:**
```bash
python scripts/load_postgres.py --truncate
python scripts/validate_phase1.py
```

**Root causes:** Stale data, interrupted load, manual DB edits.

## Phase 2 — Bronze ingestion

**Symptoms:** Missing landing paths, metadata columns absent, count mismatch.

**Recovery:**
```bash
python scripts/generate_file_sources.py
python scripts/run_local_ingestion.py
python scripts/validate_phase2.py
```

**Root causes:** Postgres not running, empty file sources, wrong `ADLS_LOCAL_LANDING_DIR`.

## Phase 4 — Silver

**Symptoms:** Quarantine rows, missing Silver tables, Spark OOM.

**Recovery:**
```bash
python scripts/run_silver_transforms.py
python scripts/validate_silver.py
```

Check quarantine at `data/lakehouse/silver/silver/_quarantine/`.

**Root causes:** Bronze DQ failures, missing Java, referential integrity violations.

## Phase 5 — Gold

**Symptoms:** Missing Gold tables, manifest errors.

**Recovery:**
```bash
python scripts/run_silver_transforms.py
python scripts/run_gold_models.py
python scripts/validate_gold.py
```

## Phase 6 — Snowflake

**Symptoms:** Connection errors, row count mismatch, missing RAW tables.

**Recovery:**
```bash
python scripts/setup_snowflake.py
python scripts/run_snowflake_load.py
python scripts/validate_snowflake.py
```

**Root causes:** Invalid `SNOWFLAKE_*` credentials, Gold tables missing.

## Phase 7 — dbt

**Symptoms:** `dbt run` fails, compile errors, test failures.

**Recovery:**
```bash
python scripts/setup_dbt_schemas.py
python scripts/run_dbt_models.py
python scripts/validate_dbt.py
```

## Phase 8 — Airflow

See [airflow-operations.md](airflow-operations.md).

## End-to-end reset (local)

```bash
docker compose up -d
python scripts/load_postgres.py --truncate
python scripts/generate_file_sources.py
python scripts/run_local_ingestion.py
python scripts/run_silver_transforms.py
python scripts/run_gold_models.py
```

Then Snowflake/dbt if credentials are configured.
