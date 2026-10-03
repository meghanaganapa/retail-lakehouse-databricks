"""Post-load data quality report.

Runs after the pipeline (locally, or as the last task of the Databricks job)
and fails loudly if the data breaks a business expectation:

* quarantine rate per source below a threshold
* order revenue reconciles with payments (excluding known quarantined rows)
* no duplicate keys in the fact table, exactly one current version per
  dimension key
* fact rows resolve to real dimension members
* data is fresh

Results are written to ``ops_quality_results`` so they can be charted on an
ops dashboard, and the process exits non-zero when a check fails, which marks
the job run as failed and triggers the failure alert.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from pyspark.sql import functions as F

from .config import MAX_QUARANTINE_RATE, MAX_RECON_MISMATCH_RATE, Settings
from .spark import get_spark
from .storage import Lakehouse


@dataclass
class Check:
    name: str
    value: float
    threshold: float
    passed: bool
    detail: str = ""


QUARANTINE_SOURCES = {
    "orders_cdc": "silver_orders_quarantine",
    "order_items": "silver_order_items_quarantine",
    "payments": "silver_payments_quarantine",
    "customers_cdc": "silver_customers_quarantine",
    "products_cdc": "silver_products_quarantine",
    "clickstream": "silver_clickstream_quarantine",
    "inventory": "silver_inventory_quarantine",
}


def run_checks(lh: Lakehouse, max_staleness_hours: float = 48.0) -> list[Check]:
    checks: list[Check] = []

    for source, qtable in QUARANTINE_SOURCES.items():
        bronze = f"bronze_{source}"
        if not lh.exists(bronze):
            continue
        total = lh.read(bronze).count()
        bad = lh.read(qtable).count() if lh.exists(qtable) else 0
        rate = bad / total if total else 0.0
        checks.append(
            Check(f"quarantine_rate.{source}", round(rate, 4), MAX_QUARANTINE_RATE, rate <= MAX_QUARANTINE_RATE, f"{bad}/{total} rows")
        )

    recon = lh.read("gold_recon_order_revenue")
    n_orders = recon.count()
    unexplained = recon.where("is_mismatch AND NOT has_quarantined_rows").count()
    rate = unexplained / n_orders if n_orders else 0.0
    checks.append(
        Check(
            "revenue_reconciliation",
            round(rate, 4),
            MAX_RECON_MISMATCH_RATE,
            rate <= MAX_RECON_MISMATCH_RATE,
            f"{unexplained}/{n_orders} orders unexplained",
        )
    )

    fact = lh.read("gold_fact_order_items")
    dupes = fact.groupBy("order_id", "order_item_id").count().where("count > 1").count()
    checks.append(Check("fact_order_items.unique_grain", dupes, 0, dupes == 0, f"{dupes} duplicate keys"))

    for dim, key in (("gold_dim_customer", "customer_id"), ("gold_dim_product", "product_id")):
        bad = lh.read(dim).where("is_current").groupBy(key).count().where("count != 1").count()
        checks.append(Check(f"{dim}.one_current_version", bad, 0, bad == 0, f"{bad} keys with != 1 current row"))

    n_fact = fact.count()
    unknown = fact.where("customer_sk = '-1' OR product_sk = '-1'").count()
    rate = unknown / n_fact if n_fact else 0.0
    checks.append(Check("fact_order_items.unknown_members", round(rate, 4), 0.01, rate <= 0.01, f"{unknown}/{n_fact} rows"))

    staleness = (
        lh.read("bronze_orders_cdc")
        .agg(((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("_ingested_at"))) / 3600).alias("h"))
        .collect()[0]["h"]
    )
    checks.append(Check("freshness_hours.orders", round(staleness, 2), max_staleness_hours, staleness <= max_staleness_hours))
    return checks


def save_results(lh: Lakehouse, checks: list[Check]) -> None:
    rows = [(c.name, float(c.value), float(c.threshold), c.passed, c.detail) for c in checks]
    df = lh.spark.createDataFrame(rows, "check_name string, value double, threshold double, passed boolean, detail string")
    lh.write(df.withColumn("checked_at", F.current_timestamp()), "ops_quality_results", mode="append")


def print_report(checks: list[Check]) -> None:
    print(f"\n{'check':<42}{'value':>10}{'limit':>10}  result")
    print("-" * 74)
    for c in checks:
        print(f"{c.name:<42}{c.value:>10}{c.threshold:>10}  {'PASS' if c.passed else 'FAIL'}  {c.detail}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run data quality checks on the lakehouse")
    parser.add_argument("--base-path", default=None)
    parser.add_argument("--storage", choices=["path", "uc"], default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--format", dest="fmt", choices=["delta", "parquet"], default=None)
    parser.add_argument("--max-staleness-hours", type=float, default=48.0)
    args = parser.parse_args(argv)

    settings = Settings.from_env(base_path=args.base_path, storage=args.storage, catalog=args.catalog, schema=args.schema, fmt=args.fmt)
    spark, fmt = get_spark("retail-lakehouse-quality", want_delta=settings.fmt == "delta")
    lh = Lakehouse(spark, Settings(**{**settings.__dict__, "fmt": fmt}))
    checks = run_checks(lh, args.max_staleness_hours)
    save_results(lh, checks)
    print_report(checks)
    failed = [c.name for c in checks if not c.passed]
    if failed:
        print(f"\n{len(failed)} check(s) failed: {', '.join(failed)}")
        sys.exit(1)
    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
