"""Spark schema to Snowflake DDL helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


def spark_type_to_snowflake(data_type: DataType) -> str:
    """Map a Spark data type to a Snowflake column type."""
    if isinstance(data_type, IntegerType):
        return "NUMBER(38,0)"
    if isinstance(data_type, LongType):
        return "NUMBER(38,0)"
    if isinstance(data_type, StringType):
        return "VARCHAR"
    if isinstance(data_type, (DoubleType, FloatType)):
        return "FLOAT"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType):
        return "TIMESTAMP_NTZ"
    if isinstance(data_type, DecimalType):
        return f"NUMBER({data_type.precision},{data_type.scale})"
    return "VARIANT"


def normalize_identifier(name: str, uppercase: bool = True) -> str:
    """Normalize a Snowflake identifier."""
    return name.upper() if uppercase else name


def generate_create_table_sql(
    table_name: str,
    df: DataFrame,
    database: str,
    schema: str,
    uppercase_identifiers: bool = True,
    if_not_exists: bool = True,
) -> str:
    """
    Generate CREATE TABLE DDL from a Spark DataFrame schema.

    Args:
        table_name: Target Snowflake table name.
        df: Source DataFrame with schema to mirror.
        database: Snowflake database name.
        schema: Snowflake schema name.
        uppercase_identifiers: Uppercase table and column names.
        if_not_exists: Include IF NOT EXISTS clause.

    Returns:
        CREATE TABLE SQL statement.
    """
    table = normalize_identifier(table_name, uppercase_identifiers)
    database_id = normalize_identifier(database, uppercase_identifiers)
    schema_id = normalize_identifier(schema, uppercase_identifiers)

    columns = []
    for field in df.schema.fields:
        column_name = normalize_identifier(field.name, uppercase_identifiers)
        column_type = spark_type_to_snowflake(field.dataType)
        nullability = "" if field.nullable else " NOT NULL"
        columns.append(f"    {column_name} {column_type}{nullability}")

    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    column_sql = ",\n".join(columns)
    return (
        f"CREATE TABLE {exists_clause}{database_id}.{schema_id}.{table} (\n"
        f"{column_sql}\n"
        f");"
    )
