"""Gold dimension table builders."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import GoldModelConfig
from retail_lakehouse.gold.schemas import (
    DIM_DATE_SCHEMA,
)


def build_dim_date(spark: SparkSession, config: GoldModelConfig) -> DataFrame:
    """
    Generate a conformed date dimension for the configured date range.

    Args:
        spark: Active Spark session.
        config: Gold model configuration.

    Returns:
        Date dimension DataFrame.
    """
    start = date.fromisoformat(config.date_start)
    end = date.fromisoformat(config.date_end)
    dates: list[date] = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)

    rows = []
    for full_date in dates:
        rows.append(
            {
                "date_key": int(full_date.strftime("%Y%m%d")),
                "full_date": full_date,
                "year": full_date.year,
                "quarter": (full_date.month - 1) // 3 + 1,
                "month": full_date.month,
                "month_name": full_date.strftime("%B"),
                "day": full_date.day,
                "day_of_week": full_date.isoweekday(),
                "day_name": full_date.strftime("%A"),
                "week_of_year": full_date.isocalendar()[1],
                "is_weekend": full_date.weekday() >= 5,
            }
        )

    df = spark.createDataFrame(rows, schema=DIM_DATE_SCHEMA)
    return df.withColumn(config.processed_at_column, F.current_timestamp())


def build_dim_customers(customers: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build customer dimension from Silver customers.

    Args:
        customers: Silver customers DataFrame.
        config: Gold model configuration.

    Returns:
        Customer dimension DataFrame.
    """
    return customers.select(
        F.col("customer_id").alias("customer_key"),
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "country_code",
        "country_name",
        "city",
        "customer_segment",
        F.to_date("signup_date").alias("signup_date"),
        "is_active",
    ).withColumn(config.processed_at_column, F.current_timestamp())


def build_dim_products(products: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build product dimension from Silver products.

    Args:
        products: Silver products DataFrame.
        config: Gold model configuration.

    Returns:
        Product dimension DataFrame.
    """
    return products.select(
        F.col("product_id").alias("product_key"),
        "product_id",
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",
        F.col("unit_price").cast("double"),
        F.col("unit_cost").cast("double"),
        "is_active",
    ).withColumn(config.processed_at_column, F.current_timestamp())


def build_dim_country(customers: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build country dimension from distinct customer countries.

    Args:
        customers: Silver customers DataFrame.
        config: Gold model configuration.

    Returns:
        Country dimension DataFrame.
    """
    return (
        customers.select("country_code", "country_name")
        .distinct()
        .withColumn("country_key", F.col("country_code"))
        .withColumn(config.processed_at_column, F.current_timestamp())
        .select(
            "country_key", "country_code", "country_name", config.processed_at_column
        )
    )
