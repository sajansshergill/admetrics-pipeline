"""Gold layer: build star-schema dimensions, the daily fact, and reporting metrics."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ---------- dimensions ----------

def dim_campaign(silver: DataFrame) -> DataFrame:
    return (
        silver.select("campaign_id", "campaign_name", "advertiser_id")
        .dropDuplicates(["campaign_id"])
    )


def dim_channel(silver: DataFrame) -> DataFrame:
    return (
        silver.select("channel")
        .dropDuplicates(["channel"])
        .withColumn("channel_key", F.sha2(F.col("channel"), 256).substr(1, 12))
    )


def dim_advertiser(silver: DataFrame) -> DataFrame:
    return (
        silver.select("advertiser_id", "advertiser_name")
        .dropDuplicates(["advertiser_id"])
    )


def dim_date(silver: DataFrame) -> DataFrame:
    return (
        silver.select("event_date")
        .dropDuplicates(["event_date"])
        .withColumn("year", F.year("event_date"))
        .withColumn("month", F.month("event_date"))
        .withColumn("day", F.dayofmonth("event_date"))
        .withColumn("day_of_week", F.dayofweek("event_date"))
    )


# ---------- fact ----------

def fct_campaign_daily(silver: DataFrame, dim_channel_df: DataFrame) -> DataFrame:
    """Grain: one row per (event_date, campaign_id, channel)."""
    agg = (
        silver.groupBy("event_date", "campaign_id", "channel")
        .agg(
            F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
            F.round(F.sum("spend"), 2).alias("spend"),
            F.round(F.sum("revenue"), 2).alias("revenue"),
        )
    )
    return agg.join(dim_channel_df.select("channel", "channel_key"), on="channel", how="left")


# ---------- reporting metrics (window functions) ----------

def campaign_metrics(fact: DataFrame) -> DataFrame:
    """Derived rates + window metrics used by the reporting layer."""
    by_campaign_time = (
        Window.partitionBy("campaign_id").orderBy("event_date")
    )
    rolling_7d = by_campaign_time.rowsBetween(-6, 0)
    by_channel_day = Window.partitionBy("channel", "event_date").orderBy(F.col("spend").desc())

    return (
        fact
        .withColumn(
            "ctr",
            F.when(F.col("impressions") > 0, F.round(F.col("clicks") / F.col("impressions"), 4)).otherwise(0.0),
        )
        .withColumn(
            "cvr",
            F.when(F.col("clicks") > 0, F.round(F.col("conversions") / F.col("clicks"), 4)).otherwise(0.0),
        )
        .withColumn(
            "roas",
            F.when(F.col("spend") > 0, F.round(F.col("revenue") / F.col("spend"), 2)).otherwise(0.0),
        )
        # 7-day rolling CTR per campaign
        .withColumn(
            "ctr_7d",
            F.round(
                F.sum("clicks").over(rolling_7d) / F.greatest(F.sum("impressions").over(rolling_7d), F.lit(1)),
                4,
            ),
        )
        # running cumulative spend per campaign
        .withColumn("running_spend", F.round(F.sum("spend").over(by_campaign_time), 2))
        # spend rank within channel for each day
        .withColumn("spend_rank_in_channel", F.rank().over(by_channel_day))
    )