"""Spark session utilities."""

from retail_lakehouse.spark.session import get_spark_session, stop_spark_session

__all__ = ["get_spark_session", "stop_spark_session"]
