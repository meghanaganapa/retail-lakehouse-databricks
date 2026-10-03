"""Batch orchestration: bronze -> silver -> gold, with run metrics.

Usage:
    run-pipeline --landing ./landing --base-path ./lakehouse_data

Every run gets a ``batch_id``. Step metrics are written to
``ops_pipeline_runs`` (rows read, written, quarantined, deduplicated) which
feeds the ops dashboard.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from .bronze import run_bronze
from .config import Settings
from .gold import run_gold
from .silver import run_silver
from .spark import get_spark
from .storage import Lakehouse


def run(lh: Lakehouse, landing_path: str) -> dict:
    batch_id = lh.next_batch_id()
    started = datetime.now(timezone.utc)
    t0 = time.time()
    bronze = run_bronze(lh, landing_path, batch_id)
    t1 = time.time()
    silver = run_silver(lh)
    t2 = time.time()
    gold = run_gold(lh)
    t3 = time.time()

    for s in silver:
        if s.get("rows_read"):
            net_new = s["rows_after"] - s["rows_before"]
            s["duplicates_or_updates_merged"] = s["rows_valid"] - net_new

    summary = {
        "batch_id": batch_id,
        "started_at": started.isoformat(timespec="seconds"),
        "format": lh.fmt,
        "seconds": {"bronze": round(t1 - t0, 1), "silver": round(t2 - t1, 1), "gold": round(t3 - t2, 1)},
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
    }
    _record_run(lh, summary)
    return summary


def _record_run(lh: Lakehouse, summary: dict) -> None:
    rows = []
    for layer in ("bronze", "silver", "gold"):
        for step in summary[layer]:
            rows.append(
                (
                    summary["batch_id"],
                    layer,
                    step["table"],
                    int(step.get("rows_read", step.get("rows", 0)) or 0),
                    int(step.get("rows_after", step.get("rows", 0)) or 0),
                    int(step.get("rows_quarantined", 0) or 0),
                    float(summary["seconds"][layer]),
                )
            )
    df = lh.spark.createDataFrame(
        rows,
        "batch_id long, layer string, table_name string, rows_in long, rows_out long, rows_quarantined long, layer_seconds double",
    ).withColumn("run_at", F.current_timestamp())
    lh.write(df, "ops_pipeline_runs", mode="append")


def print_summary(summary: dict) -> None:
    print(f"\n=== Batch {summary['batch_id']} ({summary['format']}) ===")
    print("Bronze:")
    for b in summary["bronze"]:
        drift = f"  schema drift: +{b['new_columns']}" if b.get("new_columns") else ""
        print(f"  {b['table']:<28} files={b['files']:<3} rows={b['rows']}{drift}")
    print("Silver:")
    for s in summary["silver"]:
        if not s.get("rows_read"):
            print(f"  {s['table']:<28} (no new data)")
            continue
        print(
            f"  {s['table']:<28} read={s['rows_read']:<6} quarantined={s['rows_quarantined']:<4} "
            f"merged/deduped={s['duplicates_or_updates_merged']:<5} total={s['rows_after']}"
        )
    print("Gold:")
    for g in summary["gold"]:
        print(f"  {g['table']:<28} rows={g['rows']}")
    print(f"Timing (s): {summary['seconds']}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the retail lakehouse pipeline")
    parser.add_argument("--landing", default=None, help="Landing folder with raw files")
    parser.add_argument("--base-path", default=None, help="Where tables are written (path storage)")
    parser.add_argument("--format", dest="fmt", choices=["delta", "parquet"], default=None)
    parser.add_argument("--storage", choices=["path", "uc"], default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--summary-json", default=None, help="Optional path to write the run summary")
    args = parser.parse_args(argv)

    settings = Settings.from_env(
        landing_path=args.landing,
        base_path=args.base_path,
        fmt=args.fmt,
        storage=args.storage,
        catalog=args.catalog,
        schema=args.schema,
    )
    spark, fmt = get_spark(want_delta=settings.fmt == "delta")
    lh = Lakehouse(spark, Settings(**{**settings.__dict__, "fmt": fmt}))
    summary = run(lh, settings.landing_path)
    print_summary(summary)
    if args.summary_json:
        with open(args.summary_json, "w") as f:
            json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
