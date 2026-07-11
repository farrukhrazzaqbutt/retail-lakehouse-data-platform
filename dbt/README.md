# Retail Lakehouse dbt Project

dbt models for the Retail Lakehouse Data Platform (Phase 7).

## Layers

- **staging** — views over `RETAIL_DW.RAW` Gold tables
- **intermediate** — enriched joins and aggregates
- **marts** — analytics-ready business metric tables in `RETAIL_DW.MARTS`

## Commands

```bash
# From repo root
export DBT_PROFILES_DIR=./dbt/profiles   # or set in .env

python scripts/setup_dbt_schemas.py
python scripts/run_dbt_models.py
python scripts/validate_dbt.py

# Or run dbt directly from this directory
cd dbt
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
```

See [docs/phase7-dbt-models.md](../docs/phase7-dbt-models.md) for full documentation.
