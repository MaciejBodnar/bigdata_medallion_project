from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "nyc-taxi-medallion-pipeline") -> SparkSession:
    # Keep local Spark stable on macOS setups with hostname/loopback resolution issues.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.shuffle.spill", "true")
        .config("spark.shuffle.spill.compress", "true")
        .getOrCreate()
    )
