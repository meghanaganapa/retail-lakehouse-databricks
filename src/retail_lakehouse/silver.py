"""Silver layer: typed, validated, deduplicated, conformed.

Silver is processed incrementally: each table remembers the highest bronze
``_batch_id`` it has consumed (a watermark) and only reads newer bronze rows.
Every step is idempotent, so re-running after a failure can never double-count
or corrupt history.

The core techniques, each unit-tested:

* :func:`dedupe_latest` - keep the latest version of each key, ordered by the
  source's change timestamp and then the log sequence number (LSN). This is
  the fix for duplicate and out-of-order CDC events.
* :func:`apply_scd2` - Slowly Changing Dimension Type 2 that tolerates late
  and duplicate change events by rebuilding the history of affected keys only.
* Quarantine (see :mod:`quality`), including a referential check that order
  lines point at a known product.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import quality
from .config import DEFAULT_WAREHOUSE
from .storage import Lakehouse

# --------------------------------------------------------------------- helpers


def clean_str(col: str) -> F.Column:
    """Trimmed string, with empty strings turned into NULL."""
    return F.expr(f"nullif(trim(`{col}`), '')")


def try_cast(col: str, dtype: str) -> F.Column:
    """Cast that yields NULL instead of failing, so bad values can be quarantined."""
    return F.expr(f"try_cast(nullif(trim(`{col}`), '') AS {dtype})")


META_COLS = ("_ingested_at", "_source_file", "_batch_id")


def meta_cols(df: DataFrame) -> list[str]:
    """Lineage columns present on a bronze frame (Lakeflow has no ``_batch_id``)."""
    return [c for c in META_COLS if c in df.columns]


def with_raw(df: DataFrame) -> DataFrame:
    """Keep the untouched bronze values in a struct for the quarantine table."""
    business = [c for c in df.columns if not c.startswith("_")]
    return df.withColumn("_raw", F.struct(*[F.col(c) for c in business]))


def dedupe_latest(df: DataFrame, keys: Sequence[str], order_by: Sequence[F.Column]) -> DataFrame:
    """Keep one row per key: the first according to ``order_by``."""
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def apply_scd2(
    current: DataFrame,
    changes: DataFrame,
    key: str,
    attrs: Sequence[str],
    ts_col: str = "change_ts",
    seq_col: str = "lsn",
) -> DataFrame:
    """Merge CDC ``changes`` into an SCD Type 2 dimension.

    ``changes`` has ``key``, ``attrs``, ``op`` (I/U/D), ``ts_col`` and ``seq_col``.
    Deletes become a version with ``is_deleted = true`` (soft delete), so the
    history stays complete.

    Rather than patching open rows with a MERGE (which goes wrong when an old
    event arrives late), we rebuild the version chain for *affected keys only*:
    take their existing versions plus the new events, drop exact duplicates,
    sort by event time, collapse consecutive versions with no real change, then
    recompute ``effective_to`` and ``is_current``. Running the same batch twice
    gives the same result.
    """
    tracked = [*attrs, "is_deleted"]
    incoming = changes.select(
        key,
        *attrs,
        (F.col("op") == "D").alias("is_deleted"),
        F.col(ts_col).alias("effective_from"),
        F.col(seq_col).alias("_lsn"),
    )
    affected = incoming.select(key).distinct()
    existing = current.join(affected, key, "left_semi").select(key, *tracked, "effective_from", "_lsn")

    combined = dedupe_latest(existing.unionByName(incoming), [key, "effective_from"], [F.col("_lsn").desc()]).withColumn(
        "_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in tracked]), 256)
    )

    w = Window.partitionBy(key).orderBy("effective_from", "_lsn")
    rebuilt = (
        combined.withColumn("_prev_hash", F.lag("_row_hash").over(w))
        .where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
        .drop("_prev_hash")
        .withColumn("effective_to", F.lead("effective_from").over(w))
        .withColumn("is_current", F.col("effective_to").isNull())
    )
    untouched = current.join(affected, key, "left_anti")
    cols = [key, *tracked, "effective_from", "effective_to", "is_current", "_lsn", "_row_hash"]
    return untouched.select(cols).unionByName(rebuilt.select(cols))


# ------------------------------------------------------------- typed prepares


def prepare_customers(bronze: DataFrame) -> DataFrame:
    return with_raw(bronze).select(
        clean_str("customer_id").alias("customer_id"),
        clean_str("full_name").alias("full_name"),
        F.lower(clean_str("email")).alias("email"),
        clean_str("city").alias("city"),
        clean_str("state").alias("state"),
        clean_str("segment").alias("segment"),
        F.upper(clean_str("op")).alias("op"),
        try_cast("change_ts", "timestamp").alias("change_ts"),
        try_cast("lsn", "bigint").alias("lsn"),
        "_raw",
        *meta_cols(bronze),
    )


def prepare_products(bronze: DataFrame) -> DataFrame:
    return with_raw(bronze).select(
        clean_str("product_id").alias("product_id"),
        clean_str("product_name").alias("product_name"),
        clean_str("category").alias("category"),
        try_cast("list_price", "decimal(10,2)").alias("list_price"),
        clean_str("supplier_id").alias("supplier_id"),
        F.upper(clean_str("op")).alias("op"),
        try_cast("change_ts", "timestamp").alias("change_ts"),
        try_cast("lsn", "bigint").alias("lsn"),
        "_raw",
        *meta_cols(bronze),
    )


def prepare_orders(bronze: DataFrame) -> DataFrame:
    return with_raw(bronze).select(
        clean_str("order_id").alias("order_id"),
        clean_str("customer_id").alias("customer_id"),
        F.lower(clean_str("status")).alias("status"),
        try_cast("order_purchase_ts", "timestamp").alias("order_purchase_ts"),
        try_cast("change_ts", "timestamp").alias("change_ts"),
        try_cast("lsn", "bigint").alias("lsn"),
        "_raw",
        *meta_cols(bronze),
    )


def prepare_order_items(bronze: DataFrame) -> DataFrame:
    return with_raw(bronze).select(
        clean_str("order_id").alias("order_id"),
        try_cast("order_item_id", "int").alias("order_item_id"),
        clean_str("product_id").alias("product_id"),
        try_cast("quantity", "int").alias("quantity"),
        try_cast("unit_price", "decimal(10,2)").alias("unit_price"),
        try_cast("freight_value", "decimal(10,2)").alias("freight_value"),
        "_raw",
        *meta_cols(bronze),
    )


def prepare_payments(bronze: DataFrame) -> DataFrame:
    return with_raw(bronze).select(
        clean_str("order_id").alias("order_id"),
        try_cast("payment_sequential", "int").alias("payment_sequential"),
        clean_str("payment_type").alias("payment_type"),
        try_cast("payment_installments", "int").alias("payment_installments"),
        try_cast("payment_value", "decimal(10,2)").alias("payment_value"),
        "_raw",
        *meta_cols(bronze),
    )


def prepare_clickstream(bronze: DataFrame) -> DataFrame:
    return (
        with_raw(bronze)
        .select(
            clean_str("event_id").alias("event_id"),
            clean_str("session_id").alias("session_id"),
            clean_str("customer_id").alias("customer_id"),
            clean_str("product_id").alias("product_id"),
            F.lower(clean_str("event_type")).alias("event_type"),
            try_cast("event_ts", "timestamp").alias("event_ts"),
            "_raw",
            *meta_cols(bronze),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )


def prepare_inventory(bronze: DataFrame) -> DataFrame:
    # Files sent before the schema change have no warehouse_code column at all.
    wh = clean_str("warehouse_code") if "warehouse_code" in bronze.columns else F.lit(None).cast("string")
    return with_raw(bronze).select(
        clean_str("supplier_id").alias("supplier_id"),
        clean_str("product_id").alias("product_id"),
        F.coalesce(wh, F.lit(DEFAULT_WAREHOUSE)).alias("warehouse_code"),
        try_cast("qty_on_hand", "int").alias("qty_on_hand"),
        try_cast("unit_cost", "decimal(10,2)").alias("unit_cost"),
        try_cast("snapshot_date", "date").alias("snapshot_date"),
        "_raw",
        *meta_cols(bronze),
    )


# ------------------------------------------------------------------ the runner

SCD2_ATTRS = {
    "customers": ["full_name", "email", "city", "state", "segment"],
    "products": ["product_name", "category", "list_price", "supplier_id"],
}


def _new_bronze(lh: Lakehouse, bronze_table: str, silver_table: str) -> tuple[DataFrame | None, int]:
    if not lh.exists(bronze_table):
        return None, 0
    wm = lh.watermark(silver_table)
    df = lh.read(bronze_table).where(F.col("_batch_id") > wm)
    max_batch = df.agg(F.max("_batch_id")).collect()[0][0]
    if max_batch is None:
        return None, wm
    return df, max_batch


def _quarantine(lh: Lakehouse, invalid: DataFrame, silver_table: str) -> int:
    n = invalid.count()
    if n:
        lh.write(invalid, f"{silver_table}_quarantine", mode="append")
    return n


def _process(
    lh: Lakehouse,
    entity: str,
    prepare,
    rules: dict[str, str],
    merge,
    extra_checks=None,
) -> dict:
    """Generic incremental step: new bronze -> typed -> quality -> merge -> write."""
    bronze_table = f"bronze_{entity}"
    silver_table = f"silver_{entity.replace('_cdc', '')}"
    new, max_batch = _new_bronze(lh, bronze_table, silver_table)
    if new is None:
        return {"table": silver_table, "rows_read": 0}

    typed = prepare(new)
    if extra_checks is not None:
        typed, rules = extra_checks(typed, rules)
    valid, invalid = quality.split_valid_invalid(typed, rules)
    valid = valid.cache()
    rows_read = typed.count()
    n_valid = valid.count()
    n_quarantined = _quarantine(lh, invalid, silver_table)

    rows_before = lh.read(silver_table).count() if lh.exists(silver_table) else 0
    merged = merge(valid, silver_table)
    rows_after = lh.write(merged, silver_table, mode="overwrite")
    lh.set_watermark(silver_table, max_batch)
    valid.unpersist()
    return {
        "table": silver_table,
        "rows_read": rows_read,
        "rows_valid": n_valid,
        "rows_quarantined": n_quarantined,
        "rows_before": rows_before,
        "rows_after": rows_after,
    }


def run_silver(lh: Lakehouse) -> list[dict]:
    def scd2_merge(entity: str, key: str):
        def merge(valid: DataFrame, silver_table: str) -> DataFrame:
            attrs = SCD2_ATTRS[entity]
            changes = valid.select(key, *attrs, "op", "change_ts", "lsn")
            current = lh.read(silver_table) if lh.exists(silver_table) else _empty_dim(changes, key, attrs)
            return apply_scd2(current, changes, key, attrs)

        return merge

    def latest_merge(keys: list[str], order_by: list[F.Column]):
        def merge(valid: DataFrame, silver_table: str) -> DataFrame:
            current = lh.read(silver_table) if lh.exists(silver_table) else None
            combined = valid if current is None else current.unionByName(valid, allowMissingColumns=True)
            return dedupe_latest(combined, keys, order_by)

        return merge

    def product_exists_check(typed: DataFrame, rules: dict[str, str]):
        products = lh.read("silver_products").select("product_id").distinct().withColumn("_product_known", F.lit(True))
        checked = typed.join(products, "product_id", "left").withColumn("_product_known", F.coalesce("_product_known", F.lit(False)))
        return checked, {**rules, "product_exists": "_product_known"}

    results = [
        _process(lh, "customers_cdc", prepare_customers, quality.CUSTOMER_RULES, scd2_merge("customers", "customer_id")),
        _process(lh, "products_cdc", prepare_products, quality.PRODUCT_RULES, scd2_merge("products", "product_id")),
        _process(
            lh,
            "orders_cdc",
            prepare_orders,
            quality.ORDER_RULES,
            latest_merge(["order_id"], [F.col("change_ts").desc(), F.col("lsn").desc()]),
        ),
        _process(
            lh,
            "order_items",
            prepare_order_items,
            quality.ORDER_ITEM_RULES,
            _drop_flag(latest_merge(["order_id", "order_item_id"], [F.col("_ingested_at").asc()])),
            extra_checks=product_exists_check,
        ),
        _process(
            lh,
            "payments",
            prepare_payments,
            quality.PAYMENT_RULES,
            latest_merge(["order_id", "payment_sequential"], [F.col("_ingested_at").asc()]),
        ),
        _process(
            lh,
            "clickstream",
            prepare_clickstream,
            quality.CLICK_RULES,
            latest_merge(["event_id"], [F.col("_ingested_at").asc()]),
        ),
        _process(
            lh,
            "inventory",
            prepare_inventory,
            quality.INVENTORY_RULES,
            latest_merge(["supplier_id", "product_id", "warehouse_code", "snapshot_date"], [F.col("_ingested_at").desc()]),
        ),
    ]
    return results


def _drop_flag(merge):
    def wrapped(valid: DataFrame, silver_table: str) -> DataFrame:
        return merge(valid.drop("_product_known"), silver_table)

    return wrapped


def _empty_dim(changes: DataFrame, key: str, attrs: Sequence[str]) -> DataFrame:
    spark = changes.sparkSession
    schema = changes.select(
        key,
        *attrs,
        F.lit(False).alias("is_deleted"),
        F.col("change_ts").alias("effective_from"),
        F.col("change_ts").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.col("lsn").alias("_lsn"),
        F.lit("").alias("_row_hash"),
    ).schema
    return spark.createDataFrame([], schema)
