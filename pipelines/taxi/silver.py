from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.taxi.config import TaxiPipelineConfig


def _standardize_yellow(yellow_df: DataFrame) -> DataFrame:
    return yellow_df.select(
        F.lit("yellow").alias("service_type"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
        F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("Airport_fee").cast("double").alias("airport_fee"),
        F.col("source_file_name"),
        F.col("pickup_year").cast("int").alias("pickup_year"),
        F.col("pickup_month").cast("int").alias("pickup_month"),
    )


def _standardize_green(green_df: DataFrame) -> DataFrame:
    return green_df.select(
        F.lit("green").alias("service_type"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("lpep_pickup_datetime").alias("pickup_datetime"),
        F.to_timestamp("lpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee"),
        F.col("source_file_name"),
        F.col("pickup_year").cast("int").alias("pickup_year"),
        F.col("pickup_month").cast("int").alias("pickup_month"),
    )


def _standardize_partition(service_type: str, bronze_df: DataFrame) -> DataFrame:
    if service_type == "yellow":
        standardized = _standardize_yellow(bronze_df)
    elif service_type == "green":
        standardized = _standardize_green(bronze_df)
    else:
        raise ValueError(f"Unsupported service_type: {service_type}")

    return (
        standardized.withColumn(
            "trip_duration_minutes",
            ((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60).cast("int"),
        )
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("service_type"),
                    F.col("vendor_id").cast("string"),
                    F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
                    F.date_format(F.col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
                    F.coalesce(F.col("passenger_count").cast("string"), F.lit("")),
                    F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
                    F.coalesce(F.col("rate_code_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("pu_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("do_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("payment_type").cast("string"), F.lit("")),
                    F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("pickup_datetime") <= F.col("dropoff_datetime"))
        .filter(F.col("trip_distance").isNotNull())
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("trip_distance") <= 300)
        .filter(F.col("total_amount").isNotNull())
        .filter(F.col("total_amount") > 0)
        .filter(F.col("total_amount") <= 1000)
        .filter(F.col("pu_location_id").isNotNull())
        .filter(F.col("do_location_id").isNotNull())
        .filter(F.col("trip_duration_minutes").between(1, 720))
        .dropDuplicates(["record_hash"])
        .drop("record_hash")
    )


def _read_bronze_month(spark: SparkSession, config: TaxiPipelineConfig, service_type: str, year: int, month: int) -> DataFrame:
    month_root = config.bronze_service_month_root(service_type, year, month)
    return spark.read.option("recursiveFileLookup", "true").parquet(str(month_root))


def build_silver_layer(
    spark: SparkSession,
    config: TaxiPipelineConfig,
    touched_partitions: Iterable[dict[str, int | str]],
) -> dict[str, object]:
    processed_partition_count = 0
    touched_months: set[tuple[int, int]] = set()
    total_rows = 0

    for partition in touched_partitions:
        service_type = str(partition["service_type"])
        year = int(partition["year"])
        month = int(partition["month"])
        bronze_month_df = _read_bronze_month(spark, config, service_type, year, month)
        silver_df = _standardize_partition(service_type, bronze_month_df)

        destination_path = config.silver_month_root(service_type, year, month)
        silver_df.write.mode("overwrite").parquet(str(destination_path))

        processed_partition_count += 1
        touched_months.add((year, month))
        total_rows += silver_df.count()

    return {
        "processed_partition_count": processed_partition_count,
        "touched_months": [
            {"year": year, "month": month}
            for year, month in sorted(touched_months)
        ],
        "row_count": total_rows,
    }
