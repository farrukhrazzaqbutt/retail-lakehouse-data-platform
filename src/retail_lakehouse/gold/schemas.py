"""Explicit PySpark schemas for Gold layer tables."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("is_weekend", BooleanType(), False),
    ]
)

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_key", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("email", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("city", StringType(), True),
        StructField("customer_segment", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("is_active", BooleanType(), False),
        StructField("gold_loaded_at", TimestampType(), False),
    ]
)

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_key", LongType(), False),
        StructField("product_id", LongType(), False),
        StructField("sku", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("subcategory", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("unit_price", DoubleType(), False),
        StructField("unit_cost", DoubleType(), False),
        StructField("is_active", BooleanType(), False),
        StructField("gold_loaded_at", TimestampType(), False),
    ]
)

DIM_COUNTRY_SCHEMA = StructType(
    [
        StructField("country_key", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("gold_loaded_at", TimestampType(), False),
    ]
)
