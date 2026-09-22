"""Bronze layer: land raw files as-is + add ingestion metadata."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_bronze(raw: DataFrame) -> DataFrame:
    """Add ingestion metadata. No cleaning happens here — bronze is a faithful copy."""
    return raw.withColumn("source_file", F.input_file_name()).withColumn(
        "load_ts", F.current_timestamp()
    )