"""Tests for Snowflake DDL helpers."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
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

from retail_lakehouse.warehouse.ddl import (
    generate_create_table_sql,
    spark_type_to_snowflake,
)


def test_spark_type_to_snowflake_mappings() -> None:
    assert spark_type_to_snowflake(IntegerType()) == "NUMBER(38,0)"
    assert spark_type_to_snowflake(LongType()) == "NUMBER(38,0)"
    assert spark_type_to_snowflake(StringType()) == "VARCHAR"
    assert spark_type_to_snowflake(DoubleType()) == "FLOAT"
    assert spark_type_to_snowflake(BooleanType()) == "BOOLEAN"
    assert spark_type_to_snowflake(DateType()) == "DATE"
    assert spark_type_to_snowflake(TimestampType()) == "TIMESTAMP_NTZ"


def test_generate_create_table_sql(spark_session: SparkSession) -> None:
    schema = StructType(
        [
            StructField("customer_key", LongType(), False),
            StructField("email", StringType(), True),
            StructField("is_active", BooleanType(), True),
        ]
    )
    df = spark_session.createDataFrame([], schema)
    ddl = generate_create_table_sql(
        table_name="dim_customers",
        df=df,
        database="RETAIL_DW",
        schema="RAW",
    )
    assert "CREATE TABLE IF NOT EXISTS RETAIL_DW.RAW.DIM_CUSTOMERS" in ddl
    assert "CUSTOMER_KEY NUMBER(38,0) NOT NULL" in ddl
    assert "EMAIL VARCHAR" in ddl
    assert "IS_ACTIVE BOOLEAN" in ddl
