"""Silver layer: clean, cast, dedupe, and conform."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def to_silver(bronze: DataFrame) -> DataFrame:
    """Type-cast, derive event_date, normalize text, and drop duplicate events."""
    df = (
        bronze
        # cast types
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("spend", F.col("spend").cast("double"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        # derive partition column
        .withColumn("event_date", F.to_date("event_ts"))
        # normalize categorical text
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("channel", F.initcap(F.trim("channel")))
        .withColumn("device", F.lower(F.trim("device")))
        # nulls: no negative spend/revenue, default missing money to 0
        .withColumn("spend", F.when(F.col("spend") < 0, None).otherwise(F.col("spend")))
        .fillna({"spend": 0.0, "revenue": 0.0})
    )

    # deduplicate on event_id, keeping the most recently loaded record
    w = Window.partitionBy("event_id").orderBy(F.col("load_ts").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return df.select(
        "event_id",
        "event_ts",
        "event_date",
        "event_type",
        "campaign_id",
        "campaign_name",
        "channel",
        "advertiser_id",
        "advertiser_name",
        "device",
        "spend",
        "revenue",
        "source_file",
        "load_ts",
    )