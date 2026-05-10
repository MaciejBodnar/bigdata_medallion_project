from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    primary_dataset_path = project_root / "data" / "gold" / "taxi" / "monthly_zone_metrics"
    quality_metrics_path = project_root / "artifacts" / "data_product_quality_metrics.json"

    if not primary_dataset_path.exists():
        raise SystemExit(f"Missing dataset path: {primary_dataset_path}")

    spark = (
        SparkSession.builder.appName("user-data-product-test")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(str(primary_dataset_path))

        required_columns = {
            "pickup_year",
            "pickup_month",
            "service_type",
            "pu_location_id",
            "borough",
            "zone",
            "trip_count",
            "total_revenue",
            "avg_trip_revenue",
            "avg_trip_distance",
            "avg_trip_duration_minutes",
            "total_passengers",
        }
        missing_columns = sorted(required_columns - set(df.columns))
        if missing_columns:
            raise SystemExit(f"Missing expected columns: {missing_columns}")

        row_count = df.count()
        if row_count <= 0:
            raise SystemExit("Primary data product is empty (row_count == 0)")

        mapped_rows = df.filter((F.col("borough") != "Unknown") & (F.col("zone") != "Unknown")).count()
        mapping_completeness = (mapped_rows / row_count) * 100

        top_zones = (
            df.groupBy("zone")
            .agg(F.sum("total_revenue").alias("zone_revenue"))
            .orderBy(F.col("zone_revenue").desc())
            .limit(5)
        )

        print("USER DATA PRODUCT TEST: PASS")
        print(f"Rows in monthly_zone_metrics: {row_count}")
        print(f"Zone mapping completeness: {mapping_completeness:.4f}%")
        print("Top 5 zones by revenue:")
        top_zones.show(truncate=False)

        if quality_metrics_path.exists():
            with open(quality_metrics_path, encoding="utf-8") as f:
                quality_metrics = json.load(f)

            metric_names = [m.get("metric_name") for m in quality_metrics.get("metrics", [])]
            required_metrics = {
                "zone_mapping_completeness",
                "silver_rule_compliance",
                "gold_row_count",
                "data_freshness_hours",
            }
            missing_metrics = sorted(required_metrics - set(metric_names))
            if missing_metrics:
                raise SystemExit(f"Missing quality metrics in artifact: {missing_metrics}")

            print("Quality metrics artifact check: PASS")
        else:
            print(f"Quality metrics artifact not found at: {quality_metrics_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
