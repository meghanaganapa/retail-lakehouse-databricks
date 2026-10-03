"""Spark session helper.

On Databricks a session already exists and Delta is built in. Locally we try to
configure Delta Lake through the ``delta-spark`` pip package; if the Delta jars
cannot be downloaded (offline, or Maven blocked), we fall back to Parquet so
development and tests still work.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "retail-lakehouse", want_delta: bool = True) -> tuple[SparkSession, str]:
    """Return ``(spark, table_format)`` where table_format is "delta" or "parquet"."""
    if on_databricks():
        return SparkSession.builder.getOrCreate(), "delta"

    def base() -> SparkSession.Builder:
        # A fresh builder each time: .config() mutates the builder in place, so the
        # Parquet fallback must not inherit the Delta settings.
        return (
            SparkSession.builder.appName(app_name)
            .master(os.getenv("SPARK_MASTER", "local[2]"))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.ansi.enabled", "false")
        )

    if want_delta:
        try:
            from delta import configure_spark_with_delta_pip

            builder = (
                base()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            )
            return configure_spark_with_delta_pip(builder).getOrCreate(), "delta"
        except Exception as exc:  # jar download failed, no network, etc.
            log.warning("Delta Lake unavailable (%s); falling back to Parquet.", type(exc).__name__)
    return base().getOrCreate(), "parquet"
