"""Gold fact table builders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import GoldModelConfig


def _date_key_expr(column_name: str) -> F.Column:
    """Convert a timestamp column to a YYYYMMDD date key."""
    return F.date_format(F.to_timestamp(column_name), "yyyyMMdd").cast("int")


def build_fct_orders(orders: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build orders fact table with revenue flags for downstream marts.

    Args:
        orders: Silver orders DataFrame.
        config: Gold model configuration.

    Returns:
        Orders fact DataFrame.
    """
    completed = config.completed_statuses
    cancelled = config.cancelled_statuses
    refunded = config.refunded_statuses

    return (
        orders.select(
            F.col("order_id").alias("order_key"),
            F.col("customer_id").alias("customer_key"),
            _date_key_expr("order_date").alias("order_date_key"),
            F.to_timestamp("order_date").alias("order_timestamp"),
            "order_status",
            "shipping_country",
            F.col("subtotal_amount").cast("double"),
            F.col("discount_amount").cast("double"),
            F.col("shipping_amount").cast("double"),
            F.col("tax_amount").cast("double"),
            F.col("total_amount").cast("double"),
        )
        .withColumn("is_completed", F.col("order_status").isin(completed))
        .withColumn("is_cancelled", F.col("order_status").isin(cancelled))
        .withColumn("is_refunded", F.col("order_status").isin(refunded))
        .withColumn(
            "gross_revenue",
            F.when(
                F.col("is_completed") | F.col("is_refunded"), F.col("total_amount")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "net_revenue",
            F.when(F.col("is_completed"), F.col("total_amount"))
            .when(F.col("is_refunded"), F.lit(0.0))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )


def build_fct_order_items(order_items: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build order items fact table.

    Args:
        order_items: Silver order items DataFrame.
        config: Gold model configuration.

    Returns:
        Order items fact DataFrame.
    """
    return order_items.select(
        F.col("order_item_id").alias("order_item_key"),
        F.col("order_id").alias("order_key"),
        F.col("product_id").alias("product_key"),
        F.col("quantity").cast("int"),
        F.col("unit_price").cast("double"),
        F.col("discount_pct").cast("double"),
        F.col("line_total").cast("double"),
    ).withColumn(config.processed_at_column, F.current_timestamp())


def build_fct_payments(payments: DataFrame, config: GoldModelConfig) -> DataFrame:
    """
    Build payments fact table with success/failure flags.

    Args:
        payments: Silver payments DataFrame.
        config: Gold model configuration.

    Returns:
        Payments fact DataFrame.
    """
    return (
        payments.select(
            F.col("payment_id").alias("payment_key"),
            F.col("order_id").alias("order_key"),
            _date_key_expr("payment_date").alias("payment_date_key"),
            F.to_timestamp("payment_date").alias("payment_timestamp"),
            "payment_method",
            "payment_status",
            F.col("amount").cast("double"),
        )
        .withColumn(
            "is_successful",
            F.col("payment_status").isin(config.successful_payment_statuses),
        )
        .withColumn(
            "is_failed",
            F.col("payment_status").isin(config.failed_payment_statuses),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )
