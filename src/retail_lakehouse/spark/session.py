"""Spark session management for local and Databricks execution."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "retail-lakehouse-silver",
    master: str = "local[*]",
    warehouse_dir: str | None = None,
) -> SparkSession:
    """
    Create a Spark session configured for Delta Lake Silver transforms.

    Args:
        app_name: Spark application name.
        master: Spark master URL.
        warehouse_dir: Optional Spark warehouse directory for local Delta tables.

    Returns:
        Configured SparkSession with Delta Lake extensions.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
    )

    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop an active Spark session."""
    if spark is not None:
        spark.catalog.clearCache()
        spark.stop()

    try:
        from pyspark import SparkContext

        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass
