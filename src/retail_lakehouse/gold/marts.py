"""Gold business metric mart builders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import GoldModelConfig


def build_mart_daily_sales(
    fct_orders: DataFrame,
    fct_payments: DataFrame,
    config: GoldModelConfig,
) -> DataFrame:
    """
    Build daily sales mart with revenue and payment failure metrics.

    Metrics: gross/net revenue, order counts, AOV, payment failure rate.
    """
    orders_daily = (
        fct_orders.withColumn("sale_date", F.to_date("order_timestamp"))
        .groupBy("sale_date")
        .agg(
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("net_revenue").alias("net_revenue"),
            F.count("order_key").alias("order_count"),
            F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias(
                "cancelled_order_count"
            ),
            F.sum(F.when(F.col("is_refunded"), 1).otherwise(0)).alias(
                "refunded_order_count"
            ),
        )
    )

    payments_daily = (
        fct_payments.withColumn("sale_date", F.to_date("payment_timestamp"))
        .groupBy("sale_date")
        .agg(
            F.sum(F.when(F.col("is_failed"), 1).otherwise(0)).alias(
                "payment_failure_count"
            ),
            F.count("payment_key").alias("payment_attempt_count"),
        )
    )

    return (
        orders_daily.join(payments_daily, on="sale_date", how="left")
        .fillna({"payment_failure_count": 0, "payment_attempt_count": 0})
        .withColumn(
            "average_order_value",
            F.when(
                F.col("order_count") > 0, F.col("net_revenue") / F.col("order_count")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "payment_failure_rate",
            F.when(
                F.col("payment_attempt_count") > 0,
                F.col("payment_failure_count") / F.col("payment_attempt_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "cancellation_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("cancelled_order_count") / F.col("order_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )


def build_mart_monthly_revenue(
    fct_orders: DataFrame,
    config: GoldModelConfig,
) -> DataFrame:
    """
    Build monthly revenue mart with active and new/returning customer metrics.
    """
    orders = fct_orders.withColumn(
        "year_month", F.date_format("order_timestamp", "yyyy-MM")
    )

    monthly_orders = orders.groupBy("year_month").agg(
        F.sum("gross_revenue").alias("gross_revenue"),
        F.sum("net_revenue").alias("net_revenue"),
        F.count("order_key").alias("order_count"),
        F.countDistinct("customer_key").alias("monthly_active_customers"),
    )

    first_order = orders.groupBy("customer_key").agg(
        F.min("order_timestamp").alias("first_order_timestamp")
    )
    orders_with_first = orders.join(first_order, on="customer_key", how="left")
    new_returning = (
        orders_with_first.withColumn(
            "is_new_customer_month",
            F.date_format("first_order_timestamp", "yyyy-MM") == F.col("year_month"),
        )
        .groupBy("year_month")
        .agg(
            F.countDistinct(
                F.when(F.col("is_new_customer_month"), F.col("customer_key"))
            ).alias("new_customers"),
            F.countDistinct(
                F.when(~F.col("is_new_customer_month"), F.col("customer_key"))
            ).alias("returning_customers"),
        )
    )

    return (
        monthly_orders.join(new_returning, on="year_month", how="left")
        .fillna({"new_customers": 0, "returning_customers": 0})
        .withColumn(
            "average_order_value",
            F.when(
                F.col("order_count") > 0, F.col("net_revenue") / F.col("order_count")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )


def build_mart_customer_lifetime_value(
    fct_orders: DataFrame,
    config: GoldModelConfig,
) -> DataFrame:
    """
    Build customer lifetime value mart.

    Metrics: total orders, revenue, net revenue, AOV, repeat purchase rate.
    """
    return (
        fct_orders.groupBy("customer_key")
        .agg(
            F.count("order_key").alias("total_orders"),
            F.sum("gross_revenue").alias("total_revenue"),
            F.sum("net_revenue").alias("net_revenue"),
            F.min("order_timestamp").alias("first_order_date"),
            F.max("order_timestamp").alias("last_order_date"),
        )
        .withColumn(
            "avg_order_value",
            F.when(
                F.col("total_orders") > 0, F.col("net_revenue") / F.col("total_orders")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "repeat_purchase_rate",
            F.when(F.col("total_orders") > 1, F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )


def build_mart_product_performance(
    fct_order_items: DataFrame,
    dim_products: DataFrame,
    fct_orders: DataFrame,
    config: GoldModelConfig,
) -> DataFrame:
    """
    Build product performance mart with category-level revenue metrics.
    """
    items = fct_order_items.alias("items")
    orders = fct_orders.select(
        "order_key", "net_revenue", "gross_revenue", "is_completed"
    ).alias("orders")
    products = dim_products.select(
        "product_key", "product_id", "category", "subcategory", "product_name"
    ).alias("products")

    line_revenue = (
        items.join(
            orders, F.col("items.order_key") == F.col("orders.order_key"), "inner"
        )
        .join(
            products,
            F.col("items.product_key") == F.col("products.product_key"),
            "inner",
        )
        .groupBy(
            F.col("products.product_id"),
            F.col("products.category"),
            F.col("products.subcategory"),
            F.col("products.product_name"),
        )
        .agg(
            F.sum(F.col("items.quantity")).alias("total_quantity_sold"),
            F.sum(F.col("items.line_total")).alias("gross_revenue"),
            F.sum(
                F.when(
                    F.col("orders.is_completed"), F.col("items.line_total")
                ).otherwise(F.lit(0.0))
            ).alias("net_revenue"),
            F.countDistinct(F.col("items.order_key")).alias("order_count"),
        )
        .withColumn(
            "avg_unit_revenue",
            F.when(
                F.col("total_quantity_sold") > 0,
                F.col("net_revenue") / F.col("total_quantity_sold"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )
    return line_revenue


def build_mart_customer_segments(
    fct_orders: DataFrame,
    dim_customers: DataFrame,
    config: GoldModelConfig,
) -> DataFrame:
    """
    Build customer segment mart with revenue by segment and country.

    Metrics: customer count, revenue, AOV, repeat rate, cancellation rate.
    """
    orders = fct_orders.alias("orders")
    customers = dim_customers.select(
        "customer_key", "customer_segment", "country_code", "country_name"
    ).alias("customers")

    customer_orders = orders.join(
        customers,
        F.col("orders.customer_key") == F.col("customers.customer_key"),
        "inner",
    )

    segment_metrics = customer_orders.groupBy(
        F.col("customers.customer_segment"),
        F.col("customers.country_code"),
        F.col("customers.country_name"),
    ).agg(
        F.countDistinct(F.col("customers.customer_key")).alias("customer_count"),
        F.sum("gross_revenue").alias("total_revenue"),
        F.sum("net_revenue").alias("net_revenue"),
        F.count("order_key").alias("order_count"),
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
        F.countDistinct(
            F.when(
                F.col("orders.customer_key").isNotNull(), F.col("orders.customer_key")
            )
        ).alias("ordering_customers"),
    )

    repeat_customers = (
        customer_orders.groupBy(
            F.col("customers.customer_segment"),
            F.col("customers.country_code"),
            F.col("orders.customer_key"),
        )
        .agg(F.count("order_key").alias("customer_order_count"))
        .groupBy(
            F.col("customer_segment"),
            F.col("country_code"),
        )
        .agg(
            F.sum(F.when(F.col("customer_order_count") > 1, 1).otherwise(0)).alias(
                "repeat_customers"
            ),
            F.count("*").alias("customers_with_orders"),
        )
    )

    return (
        segment_metrics.join(
            repeat_customers,
            on=["customer_segment", "country_code"],
            how="left",
        )
        .fillna({"repeat_customers": 0, "customers_with_orders": 0})
        .withColumn(
            "avg_order_value",
            F.when(
                F.col("order_count") > 0, F.col("net_revenue") / F.col("order_count")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "repeat_purchase_rate",
            F.when(
                F.col("customers_with_orders") > 0,
                F.col("repeat_customers") / F.col("customers_with_orders"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "cancellation_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("cancelled_orders") / F.col("order_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(config.processed_at_column, F.current_timestamp())
    )
