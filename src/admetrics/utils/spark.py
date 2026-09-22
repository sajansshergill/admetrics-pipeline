"""SparkSession factory — works locally (with Delta) and on Databricks."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "admetrics") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")  # small for local runs
    )
    try:
        # configure_spark_with_delta_pip is a no-op on a Databricks cluster
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        return builder.getOrCreate()