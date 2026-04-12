from __future__ import annotations

import json
from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.taxi.config import TaxiPipelineConfig


def _count_violations(silver_df: DataFrame) -> dict[str, int]:
    return {
        "null_pickup_or_dropoff": silver_df.filter(
            F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull()
        ).count(),
        "pickup_after_dropoff": silver_df.filter(F.col("pickup_datetime") > F.col("dropoff_datetime")).count(),
        "negative_trip_distance": silver_df.filter(F.col("trip_distance") < 0).count(),
        "invalid_total_amount": silver_df.filter(F.col("total_amount") <= 0).count(),
        "null_location_ids": silver_df.filter(
            F.col("pu_location_id").isNull() | F.col("do_location_id").isNull()
        ).count(),
        "invalid_duration_range": silver_df.filter(~F.col("trip_duration_minutes").between(1, 720)).count(),
        "distance_over_cap": silver_df.filter(F.col("trip_distance") > 300).count(),
        "amount_over_cap": silver_df.filter(F.col("total_amount") > 1000).count(),
    }


def run_data_quality_validation(spark: SparkSession, config: TaxiPipelineConfig) -> dict[str, object]:
    bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(str(config.bronze_taxi_root / "service_type=*"))
    bronze_zone = spark.read.parquet(config.bronze_zone_lookup_path)
    silver_df = spark.read.option("recursiveFileLookup", "true").parquet(config.silver_trips_path)
    gold_monthly = spark.read.option("recursiveFileLookup", "true").parquet(config.gold_monthly_metrics_path)
    gold_payment = spark.read.option("recursiveFileLookup", "true").parquet(config.gold_payment_metrics_path)

    row_counts = {
        "bronze_rows": bronze_df.count(),
        "bronze_zone_rows": bronze_zone.count(),
        "silver_rows": silver_df.count(),
        "gold_monthly_rows": gold_monthly.count(),
        "gold_payment_rows": gold_payment.count(),
    }

    silver_rule_violations = _count_violations(silver_df)
    unmatched_zone_rows = gold_monthly.filter(F.col("zone") == F.lit("Unknown")).count()

    critical_checks = {
        "silver_non_empty": row_counts["silver_rows"] > 0,
        "gold_monthly_non_empty": row_counts["gold_monthly_rows"] > 0,
        "silver_rules_no_violations": all(value == 0 for value in silver_rule_violations.values()),
    }

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_counts": row_counts,
        "silver_rule_violations": silver_rule_violations,
        "gold_unmatched_zone_rows": unmatched_zone_rows,
        "critical_checks": critical_checks,
    }

    config.validation_report_path.parent.mkdir(parents=True, exist_ok=True)
    config.validation_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not all(critical_checks.values()):
        raise ValueError(
            "Data quality validation failed. See report: "
            f"{config.validation_report_path}"
        )

    return report
