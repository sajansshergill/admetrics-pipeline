"""Spark schemas derived from the YAML config + star-schema key helpers."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

_TYPE_MAP = {"string": StringType()}


def bronze_struct(bronze_schema: dict) -> StructType:
    """Build a StructType for the raw read (everything lands as string first)."""
    fields = [
        StructField(col, _TYPE_MAP.get(dtype, StringType()), True)
        for col, dtype in bronze_schema["columns"].items()
    ]
    return StructType(fields)