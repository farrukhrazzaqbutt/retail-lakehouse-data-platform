-- Snowflake setup for Retail Lakehouse Data Platform (Phase 6)
-- Placeholders are rendered by scripts/setup_snowflake.py:
--   {{DATABASE}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}

-- Ensure role and warehouse context
USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};

-- Create database and RAW landing schema for Gold tables
CREATE DATABASE IF NOT EXISTS {{DATABASE}};
CREATE SCHEMA IF NOT EXISTS {{DATABASE}}.{{SCHEMA}};

USE DATABASE {{DATABASE}};
USE SCHEMA {{SCHEMA}};

-- Grant usage to the configured role
GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE {{ROLE}};
GRANT USAGE ON SCHEMA {{SCHEMA}} TO ROLE {{ROLE}};
GRANT CREATE TABLE ON SCHEMA {{SCHEMA}} TO ROLE {{ROLE}};
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {{SCHEMA}} TO ROLE {{ROLE}};
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA {{SCHEMA}} TO ROLE {{ROLE}};

-- Gold dimensions, facts, and marts are auto-created during load from Delta schemas.
-- Target tables (12 total):
--   dim_date, dim_customers, dim_products, dim_country
--   fct_orders, fct_order_items, fct_payments
--   mart_daily_sales, mart_monthly_revenue, mart_customer_lifetime_value
--   mart_product_performance, mart_customer_segments
