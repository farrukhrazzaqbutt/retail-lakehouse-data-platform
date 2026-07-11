-- Airflow metadata database (Phase 8)
-- Runs once on first PostgreSQL container startup.

CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO retail_user;
