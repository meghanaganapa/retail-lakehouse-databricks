"""Gold layer: a star schema plus business-ready aggregates.

* ``gold_dim_date``, ``gold_dim_customer``, ``gold_dim_product`` (SCD2, with
  deterministic surrogate keys so reruns produce the same keys)
* ``gold_fact_order_items`` - grain: one row per order line. Customer and
  product keys are resolved **as of the purchase time**, so a product's price
  or a customer's city is reported as it was when the order was placed.
* ``gold_fact_order_payments`` - grain: one row per order. Payments are
  aggregated *before* joining to order lines; joining raw payment rows to
  order lines multiplies revenue when an order has several payments.
* ``gold_agg_daily_sales``, ``gold_agg_inventory_status``,
  ``gold_agg_funnel_daily`` and ``gold_recon_order_revenue``.

Gold is rebuilt from silver on each run. On Databricks these are materialized
views, which refresh incrementally.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import LOW_STOCK_DAYS_OF_COVER, LOW_STOCK_MIN_UNITS, RECON_TOLERANCE
from .storage import Lakehouse

UNKNOWN_SK = "-1"  # "unknown member" for facts whose dimension row is missing
FAR_FUTURE = "9999-12-31 00:00:00"


def surrogate_key(*cols: str) -> F.Column:
    """Deterministic surrogate key (unlike monotonically_increasing_id)."""
    return F.substring(F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in cols]), 256), 1, 16)


def build_dim_date(orders: DataFrame) -> DataFrame:
    bounds = orders.agg(F.min(F.to_date("order_purchase_ts")).alias("lo"), F.max(F.to_date("order_purchase_ts")).alias("hi"))
    return bounds.select(F.explode(F.sequence("lo", "hi")).alias("date")).select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim(scd2: DataFrame, key: str, sk_name: str) -> DataFrame:
    return scd2.withColumn(sk_name, surrogate_key(key, "effective_from")).drop("_lsn", "_row_hash")


def as_of_join(fact: DataFrame, dim: DataFrame, key: str, ts_col: str, sk_name: str) -> DataFrame:
    """Attach the dimension version that was valid at ``fact[ts_col]``."""
    d = dim.select(
        F.col(key).alias("_d_key"),
        F.col(sk_name),
        F.col("effective_from").alias("_d_from"),
        F.coalesce("effective_to", F.lit(FAR_FUTURE).cast("timestamp")).alias("_d_to"),
    )
    cond = (fact[key] == d["_d_key"]) & (fact[ts_col] >= d["_d_from"]) & (fact[ts_col] < d["_d_to"])
    joined = fact.join(d, cond, "left").drop("_d_key", "_d_from", "_d_to")
    return joined.withColumn(sk_name, F.coalesce(F.col(sk_name), F.lit(UNKNOWN_SK)))


def build_fact_order_items(items: DataFrame, orders: DataFrame, dim_customer: DataFrame, dim_product: DataFrame) -> DataFrame:
    base = items.join(
        orders.select("order_id", "customer_id", "order_purchase_ts", F.col("status").alias("order_status")),
        "order_id",
        "inner",
    )
    fact = as_of_join(base, dim_customer, "customer_id", "order_purchase_ts", "customer_sk")
    fact = as_of_join(fact, dim_product, "product_id", "order_purchase_ts", "product_sk")
    return fact.select(
        "order_id",
        "order_item_id",
        F.date_format("order_purchase_ts", "yyyyMMdd").cast("int").alias("date_key"),
        "customer_sk",
        "product_sk",
        "customer_id",
        "product_id",
        "order_purchase_ts",
        "order_status",
        "quantity",
        "unit_price",
        "freight_value",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)").alias("item_revenue"),
        (F.col("quantity") * F.col("unit_price") + F.col("freight_value")).cast("decimal(12,2)").alias("line_total"),
    )


def build_fact_order_payments(payments: DataFrame) -> DataFrame:
    return payments.groupBy("order_id").agg(
        F.sum("payment_value").cast("decimal(12,2)").alias("amount_paid"),
        F.count("*").alias("payment_count"),
        F.max(F.when(F.col("payment_type") == "voucher", True).otherwise(False)).alias("used_voucher"),
        F.max("payment_installments").alias("max_installments"),
    )


def build_recon(fact_items: DataFrame, fact_payments: DataFrame, quarantined_order_ids: DataFrame) -> DataFrame:
    """Compare what was sold with what was paid, per order."""
    sold = fact_items.groupBy("order_id").agg(F.sum("line_total").cast("decimal(12,2)").alias("amount_sold"))
    q = quarantined_order_ids.distinct().withColumn("has_quarantined_rows", F.lit(True))
    return (
        sold.join(fact_payments.select("order_id", "amount_paid"), "order_id", "full")
        .join(q, "order_id", "left")
        .withColumn("has_quarantined_rows", F.coalesce("has_quarantined_rows", F.lit(False)))
        .withColumn("difference", (F.coalesce("amount_paid", F.lit(0)) - F.coalesce("amount_sold", F.lit(0))).cast("decimal(12,2)"))
        .withColumn("is_mismatch", F.abs("difference") > F.lit(RECON_TOLERANCE))
        .withColumn(
            "mismatch_reason",
            F.when(~F.col("is_mismatch"), None)
            .when(F.col("has_quarantined_rows"), "lines or payments in quarantine")
            .when(F.col("amount_paid").isNull(), "no payment")
            .when(F.col("amount_sold").isNull(), "payment without order lines")
            .otherwise("amount differs"),
        )
    )


def build_daily_sales(fact_items: DataFrame, dim_product: DataFrame) -> DataFrame:
    return (
        fact_items.where(F.col("order_status") != "canceled")
        .join(dim_product.select("product_sk", "category"), "product_sk", "left")
        .groupBy("date_key", F.coalesce("category", F.lit("unknown")).alias("category"))
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum("item_revenue").cast("decimal(14,2)").alias("revenue"),
        )
        .withColumn("avg_order_value", (F.col("revenue") / F.col("orders")).cast("decimal(12,2)"))
    )


def build_inventory_status(inventory: DataFrame, fact_items: DataFrame, dim_product: DataFrame) -> DataFrame:
    latest_date = inventory.groupBy("product_id").agg(F.max("snapshot_date").alias("snapshot_date"))
    stock = (
        inventory.join(latest_date, ["product_id", "snapshot_date"])
        .groupBy("product_id", "snapshot_date")
        .agg(F.sum("qty_on_hand").alias("qty_on_hand"), F.collect_set("warehouse_code").alias("warehouses"))
    )
    # No .collect(): this also runs as a materialized view inside Lakeflow.
    max_ts = fact_items.agg(F.max("order_purchase_ts").alias("_max_ts"))
    recent = (
        fact_items.where(F.col("order_status") != "canceled")
        .crossJoin(max_ts)
        .where(F.col("order_purchase_ts") > F.col("_max_ts") - F.expr("INTERVAL 7 DAYS"))
        .groupBy("product_id")
        .agg((F.sum("quantity") / F.lit(7.0)).alias("avg_daily_units_7d"))
    )
    current_product = dim_product.where("is_current").select("product_id", "product_name", "category", "supplier_id")
    return (
        stock.join(current_product, "product_id", "left")
        .join(recent, "product_id", "left")
        .withColumn("avg_daily_units_7d", F.round(F.coalesce("avg_daily_units_7d", F.lit(0.0)), 2))
        .withColumn(
            "days_of_cover",
            F.when(F.col("avg_daily_units_7d") > 0, F.round(F.col("qty_on_hand") / F.col("avg_daily_units_7d"), 1)),
        )
        .withColumn(
            "stock_status",
            F.when(F.col("qty_on_hand") == 0, "out_of_stock")
            .when(F.col("days_of_cover") < LOW_STOCK_DAYS_OF_COVER, "low")
            .when(F.col("qty_on_hand") <= LOW_STOCK_MIN_UNITS, "low")  # slow sellers with almost no stock
            .otherwise("ok"),
        )
    )


def build_funnel(clicks: DataFrame) -> DataFrame:
    sessions = clicks.groupBy("event_date", "session_id").agg(
        F.max(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("viewed"),
        F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carted"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchased"),
    )
    return (
        sessions.groupBy("event_date")
        .agg(
            F.count("*").alias("sessions"),
            F.sum("viewed").alias("sessions_with_view"),
            F.sum("carted").alias("sessions_with_cart"),
            F.sum("purchased").alias("sessions_with_purchase"),
        )
        .withColumn("cart_rate", F.round(F.col("sessions_with_cart") / F.col("sessions"), 4))
        .withColumn("conversion_rate", F.round(F.col("sessions_with_purchase") / F.col("sessions"), 4))
    )


def run_gold(lh: Lakehouse) -> list[dict]:
    orders = lh.read("silver_orders")
    items = lh.read("silver_order_items")
    payments = lh.read("silver_payments")

    dim_customer = build_dim(lh.read("silver_customers"), "customer_id", "customer_sk")
    dim_product = build_dim(lh.read("silver_products"), "product_id", "product_sk")
    dim_date = build_dim_date(orders)
    fact_items = build_fact_order_items(items, orders, dim_customer, dim_product)
    fact_payments = build_fact_order_payments(payments)

    q_frames = [
        lh.read(t).select(F.col("_raw.order_id").alias("order_id")).where("order_id IS NOT NULL")
        for t in ("silver_order_items_quarantine", "silver_payments_quarantine")
        if lh.exists(t)
    ]
    quarantined_ids = orders.select("order_id").limit(0)
    for qf in q_frames:
        quarantined_ids = quarantined_ids.unionByName(qf)

    outputs = {
        "gold_dim_date": dim_date,
        "gold_dim_customer": dim_customer,
        "gold_dim_product": dim_product,
        "gold_fact_order_items": fact_items,
        "gold_fact_order_payments": fact_payments,
    }
    results = [{"table": name, "rows": lh.write(df, name)} for name, df in outputs.items()]

    # Aggregates read the gold tables just written.
    fact_items = lh.read("gold_fact_order_items")
    dim_product = lh.read("gold_dim_product")
    aggs = {
        "gold_recon_order_revenue": build_recon(fact_items, lh.read("gold_fact_order_payments"), quarantined_ids),
        "gold_agg_daily_sales": build_daily_sales(fact_items, dim_product),
        "gold_agg_inventory_status": build_inventory_status(lh.read("silver_inventory"), fact_items, dim_product),
        "gold_agg_funnel_daily": build_funnel(lh.read("silver_clickstream")),
    }
    results += [{"table": name, "rows": lh.write(df, name)} for name, df in aggs.items()]
    return results
