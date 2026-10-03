"""Data quality rules and quarantine.

Rules are plain SQL boolean expressions keyed by name. The same dictionaries
are used in two places:

* locally / in tests, by :func:`split_valid_invalid`, and
* on Databricks, by ``@dlt.expect_all_or_drop(...)`` in the Lakeflow pipeline,

so the definition of "good data" lives in exactly one place.

Bad rows are never silently dropped: they go to a ``*_quarantine`` table with
the original raw values and the list of rules they failed.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import VALID_EVENT_TYPES, VALID_ORDER_STATUSES


def _in(col: str, values: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{col} IN ({quoted})"


ORDER_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_status": _in("status", VALID_ORDER_STATUSES),
    "purchase_ts_parsed": "order_purchase_ts IS NOT NULL",
    "change_ts_parsed": "change_ts IS NOT NULL",
}

ORDER_ITEM_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "price_parsed": "unit_price IS NOT NULL",
    "price_positive": "unit_price > 0",
    "quantity_positive": "quantity > 0",
    "freight_non_negative": "freight_value >= 0",
}

PAYMENT_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "amount_parsed": "payment_value IS NOT NULL",
    "amount_non_negative": "payment_value >= 0",
}

CUSTOMER_RULES = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_op": "op IN ('I', 'U', 'D')",
    "change_ts_parsed": "change_ts IS NOT NULL",
}

PRODUCT_RULES = {
    "product_id_not_null": "product_id IS NOT NULL",
    "valid_op": "op IN ('I', 'U', 'D')",
    "price_positive": "list_price > 0",
    "change_ts_parsed": "change_ts IS NOT NULL",
}

CLICK_RULES = {
    "event_id_not_null": "event_id IS NOT NULL",
    "valid_event_type": _in("event_type", VALID_EVENT_TYPES),
    "event_ts_parsed": "event_ts IS NOT NULL",
}

INVENTORY_RULES = {
    "product_id_not_null": "product_id IS NOT NULL",
    "qty_non_negative": "qty_on_hand >= 0",
    "snapshot_date_parsed": "snapshot_date IS NOT NULL",
}


def failed_rules_col(rules: dict[str, str]) -> Column:
    """Array of the names of rules a row fails. A rule evaluating to NULL fails."""
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def split_valid_invalid(df: DataFrame, rules: dict[str, str]) -> tuple[DataFrame, DataFrame]:
    """Split ``df`` into (valid, quarantine).

    If ``df`` has a ``_raw`` struct column (the untouched bronze values), the
    quarantine frame keeps it so analysts can see exactly what arrived.
    """
    checked = df.withColumn("_failed_rules", failed_rules_col(rules))
    valid = checked.where(F.size("_failed_rules") == 0).drop("_failed_rules", "_raw")
    invalid = checked.where(F.size("_failed_rules") > 0)
    if "_raw" in df.columns:
        meta = [c for c in df.columns if c.startswith("_") and c != "_raw"]
        invalid = invalid.select("_raw", "_failed_rules", *meta)
    invalid = invalid.withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


def rule_failure_counts(quarantine: DataFrame) -> dict[str, int]:
    rows = quarantine.select(F.explode("_failed_rules").alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: r["count"] for r in rows}
