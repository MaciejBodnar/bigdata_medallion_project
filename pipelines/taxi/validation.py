from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.taxi.config import TaxiPipelineConfig
from scripts.generate_quality_report import generate_quality_report


def _silver_compliant_predicate() -> F.Column:
    return (
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & (F.col("pickup_datetime") <= F.col("dropoff_datetime"))
        & F.col("trip_distance").isNotNull()
        & F.col("trip_distance").between(0, 300)
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") > 0)
        & (F.col("total_amount") <= 1000)
        & F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
        & F.col("trip_duration_minutes").between(1, 720)
    )


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


def _hours_since_last_modification(paths: list[Path]) -> float | None:
    latest_modified: datetime | None = None

    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            candidate = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            latest_modified = max(latest_modified, candidate) if latest_modified else candidate
            continue

        for file_path in path.rglob("*"):
            if not file_path.is_file():
                continue
            candidate = datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC)
            latest_modified = max(latest_modified, candidate) if latest_modified else candidate

    if latest_modified is None:
        return None

    age_seconds = (datetime.now(UTC) - latest_modified).total_seconds()
    return round(age_seconds / 3600, 4)


def _build_quality_metrics(
    config: TaxiPipelineConfig,
    row_counts: dict[str, int],
    silver_df: DataFrame,
    silver_rule_violations: dict[str, int],
    gold_monthly: DataFrame,
) -> dict[str, object]:
    gold_non_unknown_rows = gold_monthly.filter(
        (F.col("borough") != F.lit("Unknown")) & (F.col("zone") != F.lit("Unknown"))
    ).count()
    zone_mapping_completeness = (
        round((gold_non_unknown_rows / row_counts["gold_monthly_rows"]) * 100, 4)
        if row_counts["gold_monthly_rows"] > 0
        else 0.0
    )

    silver_compliant_rows = silver_df.filter(_silver_compliant_predicate()).count()
    silver_rule_compliance = (
        round((silver_compliant_rows / row_counts["silver_rows"]) * 100, 4)
        if row_counts["silver_rows"] > 0
        else 0.0
    )

    freshness_hours = _hours_since_last_modification(
        [
            Path(config.gold_monthly_metrics_path),
            config.validation_report_path,
        ]
    )

    metrics = [
        {
            "metric_name": "zone_mapping_completeness",
            "metric_definition": "Percent of records in gold.monthly_zone_metrics where borough and zone are not Unknown.",
            "current_value": zone_mapping_completeness,
            "expected_threshold": ">= 99.5",
            "update_cadence": "Every pipeline run",
            "unit": "percent",
            "meets_threshold": zone_mapping_completeness >= 99.5,
        },
        {
            "metric_name": "silver_rule_compliance",
            "metric_definition": "Percent of silver records compliant with business quality rules.",
            "current_value": silver_rule_compliance,
            "expected_threshold": "100",
            "update_cadence": "Every pipeline run",
            "unit": "percent",
            "meets_threshold": silver_rule_compliance == 100.0,
            "details": {
                "violations": silver_rule_violations,
            },
        },
        {
            "metric_name": "gold_row_count",
            "metric_definition": "Total number of records in gold.monthly_zone_metrics.",
            "current_value": row_counts["gold_monthly_rows"],
            "expected_threshold": "> 0",
            "update_cadence": "Every pipeline run",
            "unit": "rows",
            "meets_threshold": row_counts["gold_monthly_rows"] > 0,
        },
        {
            "metric_name": "data_freshness_hours",
            "metric_definition": "Hours since last modification of gold.monthly_zone_metrics or validation report artifact.",
            "current_value": freshness_hours,
            "expected_threshold": "< 168",
            "update_cadence": "Every pipeline run",
            "unit": "hours",
            "meets_threshold": bool(freshness_hours is not None and freshness_hours < 168),
        },
    ]

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "primary_data_product": "gold.monthly_zone_metrics",
        "supporting_dataset": ["gold.payment_type_metrics"],
        "metrics": metrics,
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
    quality_metrics = _build_quality_metrics(config, row_counts, silver_df, silver_rule_violations, gold_monthly)

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
        "data_product_quality_metrics": quality_metrics["metrics"],
        "data_product_quality_metrics_path": str(config.artifacts_dir / "data_product_quality_metrics.json"),
    }

    config.validation_report_path.parent.mkdir(parents=True, exist_ok=True)
    config.validation_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    metrics_json_path = config.artifacts_dir / "data_product_quality_metrics.json"
    metrics_json_path.write_text(
        json.dumps(quality_metrics, indent=2),
        encoding="utf-8",
    )

    try:
        generate_quality_report(metrics_json_path, config.artifacts_dir)
    except Exception as e:
        print(f"Warning: Failed to generate quality report: {e}")

    if not all(critical_checks.values()):
        raise ValueError(
            "Data quality validation failed. See report: "
            f"{config.validation_report_path}"
        )

    return report
