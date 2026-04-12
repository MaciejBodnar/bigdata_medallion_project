from __future__ import annotations

import re
import shutil
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.taxi.config import TaxiPipelineConfig


def _extract_year_month_from_filename(
    file_name: str,
    default_year: int,
    default_month: int,
) -> tuple[int, int]:
    match = re.search(r"(\d{4})-(\d{2})", file_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    return default_year, default_month


def _bronze_file_partition_path(base_dir, service_type: str, year: int, month: int):
    return (
        base_dir
        / f"service_type={service_type}"
        / f"pickup_year={year}"
        / f"pickup_month={month:02d}"
    )


def _enrich_bronze_frame(
    source_df: DataFrame,
    service_type: str,
    source_file_name: str,
    year: int,
    month: int,
) -> DataFrame:
    return (
        source_df.withColumn("service_type", F.lit(service_type))
        .withColumn("source_file_name", F.lit(source_file_name))
        .withColumn("ingested_at_utc", F.current_timestamp())
        .withColumn("pickup_year", F.lit(year).cast("int"))
        .withColumn("pickup_month", F.lit(month).cast("int"))
    )

def build_bronze_layer(spark: SparkSession, config: TaxiPipelineConfig) -> dict[str, Any]:
    touched_partitions: set[tuple[str, int, int]] = set()
    cleared_partitions: set[tuple[str, int, int]] = set()
    processed_file_count = 0
    skipped_file_count = 0
    processed_master_files = 0

    for service_type in config.services:
        service_dir = config.raw_dir / service_type
        for source_file in sorted(service_dir.glob("*.parquet")):
            year, month = _extract_year_month_from_filename(
                source_file.name,
                default_year=min(config.years),
                default_month=min(config.months),
            )
            if year not in config.years or month not in config.months:
                skipped_file_count += 1
                continue

            raw_df = spark.read.parquet(str(source_file))
            enriched_df = _enrich_bronze_frame(
                raw_df,
                service_type=service_type,
                source_file_name=source_file.name,
                year=year,
                month=month,
            )
            destination_path = _bronze_file_partition_path(
                config.bronze_taxi_root,
                service_type,
                year,
                month,
            )
            partition_key = (service_type, year, month)
            if partition_key not in cleared_partitions:
                shutil.rmtree(destination_path, ignore_errors=True)
                cleared_partitions.add(partition_key)

            enriched_df.write.mode("append").parquet(str(destination_path))
            touched_partitions.add((service_type, year, month))
            processed_file_count += 1

    zone_lookup_source = config.raw_dir / "taxi_zone_lookup.csv"
    if zone_lookup_source.exists():
        zone_df = spark.read.option("header", True).csv(str(zone_lookup_source))
        zone_destination = config.bronze_taxi_root / "taxi_zone_lookup"
        zone_df.write.mode("overwrite").parquet(str(zone_destination))
        processed_master_files = 1

    return {
        "processed_file_count": processed_file_count,
        "skipped_file_count": skipped_file_count,
        "processed_master_files": processed_master_files,
        "touched_partitions": [
            {"service_type": service_type, "year": year, "month": month}
            for service_type, year, month in sorted(touched_partitions)
        ],
    }
