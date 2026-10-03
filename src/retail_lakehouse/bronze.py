"""Bronze layer: land raw data as-is, plus lineage metadata.

Rules for bronze:
* Never transform business values. Everything is kept as a string, exactly as
  the source sent it, so silver can always be rebuilt from bronze.
* Append-only, with ``_ingested_at``, ``_source_file`` and ``_batch_id`` so any
  record can be traced back to the file and run that delivered it.
* Each landing file is ingested exactly once (tracked in ``ops_ingested_files``),
  which is what Auto Loader's checkpoint does for us on Databricks.
* Schema drift is tolerated: new columns are added (``unionByName`` with
  ``allowMissingColumns`` locally, ``mergeSchema``/schema evolution on Delta).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .storage import Lakehouse


@dataclass(frozen=True)
class Source:
    name: str  # folder under landing/ and suffix of the bronze table
    fmt: str  # "json" | "csv"

    @property
    def table(self) -> str:
        return f"bronze_{self.name}"


SOURCES = (
    Source("customers_cdc", "json"),
    Source("products_cdc", "json"),
    Source("orders_cdc", "json"),
    Source("order_items", "csv"),
    Source("payments", "csv"),
    Source("clickstream", "json"),
    Source("inventory", "csv"),
)


def list_landing_files(landing_path: str, source: Source) -> list[str]:
    folder = Path(landing_path) / source.name
    if not folder.exists():
        return []
    ext = ".json" if source.fmt == "json" else ".csv"
    return sorted(str(p.resolve()) for p in folder.glob(f"*{ext}"))


def read_raw(lh: Lakehouse, path: str, fmt: str) -> DataFrame:
    """Read one landing file with every column as a string."""
    reader = lh.spark.read
    if fmt == "csv":
        df = reader.option("header", "true").option("inferSchema", "false").csv(path)
    else:
        # primitivesAsString keeps numbers as text, matching CSV behaviour.
        df = reader.option("primitivesAsString", "true").json(path)
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])


def add_metadata(df: DataFrame, source_file: str, batch_id: int) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id).cast("long"))
    )


def union_drifting(frames: list[DataFrame]) -> DataFrame:
    """Union frames whose columns differ; missing columns become null."""
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def ingest_source(lh: Lakehouse, landing_path: str, source: Source, batch_id: int) -> dict:
    already = lh.ingested_files()
    new_files = [f for f in list_landing_files(landing_path, source) if f not in already]
    if not new_files:
        return {"table": source.table, "files": 0, "rows": 0}

    frames = [add_metadata(read_raw(lh, f, source.fmt), f, batch_id) for f in new_files]
    batch_df = union_drifting(frames)
    new_cols: list[str] = []
    if lh.exists(source.table):
        # Align with existing bronze columns so appends never drop data.
        existing = lh.read(source.table).limit(0)
        batch_df = existing.unionByName(batch_df, allowMissingColumns=True)
        new_cols = [c for c in batch_df.columns if c not in existing.columns]
        if new_cols and lh.fmt == "parquet" and lh.settings.storage == "path":
            # Parquet has no schema evolution on append: rewrite with the wider schema.
            widened = lh.read(source.table).unionByName(batch_df, allowMissingColumns=True)
            lh.write(widened, source.table, mode="overwrite")
            lh.mark_ingested(new_files, source.name, batch_id)
            return {"table": source.table, "files": len(new_files), "rows": batch_df.count(), "new_columns": new_cols}

    rows = batch_df.count()
    lh.write(batch_df, source.table, mode="append")
    lh.mark_ingested(new_files, source.name, batch_id)
    result = {"table": source.table, "files": len(new_files), "rows": rows}
    if new_cols:
        result["new_columns"] = new_cols
    return result


def run_bronze(lh: Lakehouse, landing_path: str, batch_id: int) -> list[dict]:
    return [ingest_source(lh, landing_path, s, batch_id) for s in SOURCES]
