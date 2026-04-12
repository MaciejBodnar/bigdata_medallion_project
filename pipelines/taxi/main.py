from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from pipelines.taxi.bronze import build_bronze_layer
from pipelines.taxi.config import DEFAULT_SERVICES, TaxiPipelineConfig
from pipelines.taxi.gold import build_gold_layer
from pipelines.taxi.ingestion import assert_minimum_input_files, download_source_files
from pipelines.taxi.silver import build_silver_layer
from pipelines.taxi.spark_session import create_spark_session
from pipelines.taxi.validation import run_data_quality_validation


@task(name="prepare-directories")
def prepare_directories(config_raw: dict[str, Any]) -> None:
    config = TaxiPipelineConfig.from_dict(config_raw)
    for directory in [
        config.raw_dir,
        config.bronze_dir,
        config.silver_dir,
        config.gold_dir,
        config.artifacts_dir,
        config.bronze_taxi_root,
        config.silver_taxi_root,
        config.gold_taxi_root,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


@task(name="download-source-files")
def download_inputs(config_raw: dict[str, Any]) -> dict[str, Any]:
    config = TaxiPipelineConfig.from_dict(config_raw)
    if not config.run_download:
        return {"downloaded_files": 0, "skipped_files": 0, "failed_url_count": 0, "failed_urls": []}
    return download_source_files(config)


@task(name="build-bronze-layer")
def run_bronze(config_raw: dict[str, Any]) -> dict[str, Any]:
    config = TaxiPipelineConfig.from_dict(config_raw)
    spark = create_spark_session()
    try:
        assert_minimum_input_files(config)
        return build_bronze_layer(spark, config)
    finally:
        spark.stop()


@task(name="build-silver-layer")
def run_silver(config_raw: dict[str, Any], bronze_result: dict[str, Any]) -> dict[str, Any]:
    config = TaxiPipelineConfig.from_dict(config_raw)
    partitions = list(bronze_result["touched_partitions"])
    spark = create_spark_session()
    try:
        return build_silver_layer(spark, config, partitions)
    finally:
        spark.stop()


@task(name="build-gold-layer")
def run_gold(config_raw: dict[str, Any], silver_result: dict[str, Any]) -> dict[str, Any]:
    config = TaxiPipelineConfig.from_dict(config_raw)
    months = list(silver_result["touched_months"])
    spark = create_spark_session()
    try:
        return build_gold_layer(spark, config, months)
    finally:
        spark.stop()


@task(name="run-data-quality-validation")
def run_validation(config_raw: dict[str, Any]) -> dict[str, object]:
    config = TaxiPipelineConfig.from_dict(config_raw)
    spark = create_spark_session()
    try:
        return run_data_quality_validation(spark, config)
    finally:
        spark.stop()


@flow(name="nyc-tlc-prefect-spark-pipeline", log_prints=True)
def taxi_pipeline_flow(config_raw: dict[str, Any]) -> dict[str, Any]:
    logger = get_run_logger()
    prepare_directories(config_raw)
    download_stats = download_inputs(config_raw)
    logger.info("download stats: %s", download_stats)

    bronze_stats = run_bronze(config_raw)
    silver_stats = run_silver(config_raw, bronze_stats)
    gold_stats = run_gold(config_raw, silver_stats)
    validation_report = run_validation(config_raw)

    summary = {
        "download": download_stats,
        "bronze": bronze_stats,
        "silver": silver_stats,
        "gold": gold_stats,
        "validation": validation_report,
    }
    logger.info("pipeline completed with summary: %s", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NYC TLC Medallion pipeline with Prefect + PySpark.")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--years", nargs="+", type=int, default=[2025])
    parser.add_argument("--months", nargs="+", type=int, default=list(range(1, 13)))
    parser.add_argument("--services", nargs="+", default=list(DEFAULT_SERVICES), choices=list(DEFAULT_SERVICES))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--sample", action="store_true", help="Shortcut for a small local run: first month of the first year.")
    return parser.parse_args()


def _build_config_from_args(args: argparse.Namespace) -> TaxiPipelineConfig:
    months = args.months
    years = args.years
    if args.sample:
        years = [min(args.years)]
        months = [1]

    return TaxiPipelineConfig.create(
        project_root=Path(args.project_root),
        years=years,
        months=months,
        services=list(args.services),
        run_download=not args.skip_download,
    )


def main() -> None:
    args = _parse_args()
    config = _build_config_from_args(args)
    taxi_pipeline_flow(config_raw=config.to_dict())


if __name__ == "__main__":
    main()
