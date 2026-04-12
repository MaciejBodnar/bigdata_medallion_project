from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.taxi.config import TaxiPipelineConfig


def _read_silver_month(spark: SparkSession, config: TaxiPipelineConfig, year: int, month: int) -> DataFrame:
    return spark.read.option("recursiveFileLookup", "true").parquet(config.silver_month_glob(year, month))


def build_gold_layer(
    spark: SparkSession,
    config: TaxiPipelineConfig,
    touched_months: Iterable[dict[str, int]],
) -> dict[str, object]:
    zone_df = spark.read.parquet(config.bronze_zone_lookup_path)
    zone_lookup = zone_df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
    )

    processed_month_count = 0
    total_rows = 0

    for month_partition in touched_months:
        year = int(month_partition["year"])
        month = int(month_partition["month"])
        silver_month_df = _read_silver_month(spark, config, year, month)
        if silver_month_df.rdd.isEmpty():
            continue

        monthly_zone_metrics = (
            silver_month_df.alias("s")
            .join(zone_lookup.alias("z"), F.col("s.pu_location_id") == F.col("z.location_id"), "left")
            .groupBy(
                F.col("s.pickup_year").alias("pickup_year"),
                F.col("s.pickup_month").alias("pickup_month"),
                F.col("s.service_type").alias("service_type"),
                F.col("s.pu_location_id").alias("pu_location_id"),
                F.coalesce(F.col("z.borough"), F.lit("Unknown")).alias("borough"),
                F.coalesce(F.col("z.zone"), F.lit("Unknown")).alias("zone"),
            )
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum("s.total_amount").alias("total_revenue"),
                F.avg("s.total_amount").alias("avg_trip_revenue"),
                F.avg("s.trip_distance").alias("avg_trip_distance"),
                F.avg("s.trip_duration_minutes").alias("avg_trip_duration_minutes"),
                F.sum(F.coalesce(F.col("s.passenger_count"), F.lit(0))).alias("total_passengers"),
            )
        )

        payment_type_metrics = silver_month_df.groupBy("pickup_year", "pickup_month", "service_type", "payment_type").agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_trip_revenue"),
        )

        monthly_zone_metrics.write.mode("overwrite").parquet(str(config.gold_monthly_root(year, month)))
        payment_type_metrics.write.mode("overwrite").parquet(str(config.gold_payment_root(year, month)))

        processed_month_count += 1
        total_rows += monthly_zone_metrics.count() + payment_type_metrics.count()

    return {
        "processed_month_count": processed_month_count,
        "row_count": total_rows,
    }
