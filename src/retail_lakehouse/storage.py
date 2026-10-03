"""Table I/O, independent of where tables live.

``Lakehouse`` reads and writes tables by logical name (``silver_orders``) and
hides whether they are Delta folders, Parquet folders or Unity Catalog tables.
It also keeps two small bits of state that make the pipeline incremental and
restartable: a log of ingested landing files and per-table watermarks.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings


class Lakehouse:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings
        self.fmt = settings.fmt

    # ------------------------------------------------------------------ tables
    def exists(self, name: str) -> bool:
        tid = self.settings.table_id(name)
        if self.settings.storage == "uc":
            return self.spark.catalog.tableExists(tid)
        return Path(tid).exists() and any(Path(tid).iterdir())

    def read(self, name: str) -> DataFrame:
        tid = self.settings.table_id(name)
        if self.settings.storage == "uc":
            return self.spark.table(tid)
        return self.spark.read.format(self.fmt).load(tid)

    def read_or_empty(self, name: str, like: DataFrame) -> DataFrame:
        """Read a table, or return an empty frame with ``like``'s schema."""
        if self.exists(name):
            return self.read(name)
        return self.spark.createDataFrame([], like.schema)

    def write(self, df: DataFrame, name: str, mode: str = "overwrite") -> int:
        """Write a table and return its row count after the write.

        Overwrites are safe even when ``df`` was derived from the same table:
        Delta and Unity Catalog give snapshot isolation, and for the Parquet
        fallback we write to a staging folder and swap it in.
        """
        tid = self.settings.table_id(name)
        if self.settings.storage == "uc":
            (
                df.write.format("delta")
                .mode(mode)
                .option("mergeSchema", "true")
                .option("overwriteSchema", "true" if mode == "overwrite" else "false")
                .saveAsTable(tid)
            )
        elif self.fmt == "delta":
            writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
            if mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")
            writer.save(tid)
        else:
            if mode == "overwrite" and self.exists(name):
                staging = f"{tid}__staging_{uuid.uuid4().hex[:8]}"
                df.write.format("parquet").mode("overwrite").save(staging)
                shutil.rmtree(tid)
                Path(staging).rename(tid)
            else:
                df.write.format("parquet").mode(mode).save(tid)
        return self.read(name).count()

    # ------------------------------------------------------------- ingest log
    def ingested_files(self) -> set[str]:
        if not self.exists("ops_ingested_files"):
            return set()
        return {r.file_path for r in self.read("ops_ingested_files").select("file_path").collect()}

    def mark_ingested(self, files: list[str], source: str, batch_id: int) -> None:
        if not files:
            return
        rows = [(f, source, batch_id) for f in files]
        df = self.spark.createDataFrame(rows, "file_path string, source string, batch_id long").withColumn(
            "ingested_at", F.current_timestamp()
        )
        self.write(df, "ops_ingested_files", mode="append")

    # ------------------------------------------------------------- watermarks
    def watermark(self, table: str) -> int:
        """Highest bronze ``_batch_id`` already processed into ``table``."""
        if not self.exists("ops_watermarks"):
            return 0
        rows = self.read("ops_watermarks").where(F.col("table_name") == table).agg(F.max("batch_id")).collect()
        return rows[0][0] or 0

    def set_watermark(self, table: str, batch_id: int) -> None:
        df = self.spark.createDataFrame([(table, batch_id)], "table_name string, batch_id long").withColumn(
            "updated_at", F.current_timestamp()
        )
        self.write(df, "ops_watermarks", mode="append")

    def next_batch_id(self) -> int:
        if not self.exists("ops_ingested_files"):
            return 1
        return (self.read("ops_ingested_files").agg(F.max("batch_id")).collect()[0][0] or 0) + 1
