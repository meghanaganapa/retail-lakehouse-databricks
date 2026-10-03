# Databricks notebook source
"""Lakeflow Declarative Pipeline (formerly Delta Live Tables) for the retail lakehouse.

This is the production version of the medallion flow. It reuses the business
logic from the ``retail_lakehouse`` package (typing, quality rules, gold
builders), so what is unit-tested locally is what runs on Databricks.

Bronze  Auto Loader streaming tables (exactly-once file ingest, schema evolution),
        optional Event Hubs (Kafka API) source for clickstream.
Silver  Expectations drop bad rows; failing rows are kept in *_quarantine tables.
        AUTO CDC flows handle out-of-order CDC: SCD1 for orders and facts,
        SCD2 for customers and products.
Gold    Materialized views: star schema, reconciliation, aggregates.

Pipeline configuration (set in resources/retail_pipeline.yml):
    bundle.sourcePath     workspace path of ./src (so the package is importable)
    landing_path          e.g. /Volumes/retail_dev/landing/raw
    clickstream_source    "files" (default) or "eventhubs"
    eventhubs.namespace / eventhubs.name / eventhubs.secret_scope / eventhubs.secret_key
"""

import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("bundle.sourcePath", "."))  # noqa: F821  (spark is provided by the runtime)

from retail_lakehouse import gold, quality  # noqa: E402
from retail_lakehouse import silver as s  # noqa: E402

LANDING = spark.conf.get("landing_path")  # noqa: F821
CLICKSTREAM_SOURCE = spark.conf.get("clickstream_source", "files")  # noqa: F821


# =============================================================================
# Bronze: raw, append-only, all strings, with lineage columns
# =============================================================================


def autoloader(folder: str, fmt: str):
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "false")  # keep everything as text in bronze
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # supplier schema drift
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    return (
        reader.load(f"{LANDING}/{folder}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def bronze_table(name: str, fmt: str):
    @dlt.table(
        name=f"bronze_{name}",
        comment=f"Raw {name} as delivered, ingested once per file by Auto Loader.",
        table_properties={"quality": "bronze"},
    )
    def _t():
        return autoloader(name, fmt)


for _name, _fmt in [
    ("customers_cdc", "json"),
    ("products_cdc", "json"),
    ("orders_cdc", "json"),
    ("order_items", "csv"),
    ("payments", "csv"),
    ("inventory", "csv"),
]:
    bronze_table(_name, _fmt)


@dlt.table(name="bronze_clickstream", comment="Raw clickstream events (files or Event Hubs).", table_properties={"quality": "bronze"})
def bronze_clickstream():
    if CLICKSTREAM_SOURCE != "eventhubs":
        return autoloader("clickstream", "json")

    # Event Hubs exposes a Kafka-compatible endpoint. The connection string
    # comes from a Key Vault-backed secret scope: never hard-coded.
    namespace = spark.conf.get("eventhubs.namespace")  # noqa: F821
    hub = spark.conf.get("eventhubs.name")  # noqa: F821
    conn = dbutils.secrets.get(spark.conf.get("eventhubs.secret_scope"), spark.conf.get("eventhubs.secret_key"))  # noqa: F821
    jaas = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{conn}";'
    schema = "event_id string, session_id string, customer_id string, product_id string, event_type string, event_ts string"
    return (
        spark.readStream.format("kafka")  # noqa: F821
        .option("kafka.bootstrap.servers", f"{namespace}.servicebus.windows.net:9093")
        .option("subscribe", hub)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("e"), "topic", "partition", "offset")
        .select("e.*", F.concat_ws(":", "topic", "partition", "offset").alias("_source_file"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# =============================================================================
# Silver: typed views -> quarantine + AUTO CDC targets
# =============================================================================


def quarantine_table(entity: str, typed_view: str, rules: dict):
    @dlt.table(name=f"silver_{entity}_quarantine", comment=f"Rows from {entity} that failed a quality rule.")
    def _q():
        return (
            dlt.read_stream(typed_view)
            .withColumn("_failed_rules", quality.failed_rules_col(rules))
            .where(F.size("_failed_rules") > 0)
            .select("_raw", "_failed_rules", "_source_file", "_ingested_at")
            .withColumn("_quarantined_at", F.current_timestamp())
        )


def typed_view(name: str, source: str, prepare, extra=None):
    @dlt.view(name=name)
    def _v():
        df = prepare(dlt.read_stream(source))
        return extra(df) if extra else df


def valid_view(name: str, typed: str, rules: dict, drop_cols=("_raw",)):
    @dlt.view(name=name)
    @dlt.expect_all_or_drop(rules)
    def _v():
        return dlt.read_stream(typed).drop(*drop_cols)


# ---- customers & products: SCD Type 2 ----------------------------------------
for entity, prep, rules, key in [
    ("customers", s.prepare_customers, quality.CUSTOMER_RULES, "customer_id"),
    ("products", s.prepare_products, quality.PRODUCT_RULES, "product_id"),
]:
    typed_view(f"{entity}_typed", f"bronze_{entity}_cdc", prep, extra=lambda df: df.withColumn("is_deleted", F.col("op") == "D"))
    quarantine_table(entity, f"{entity}_typed", rules)
    valid_view(f"{entity}_valid", f"{entity}_typed", rules)
    dlt.create_streaming_table(name=f"silver_{entity}", comment=f"{entity} with full SCD2 history (soft deletes as is_deleted).")
    dlt.create_auto_cdc_flow(  # the new name for dlt.apply_changes
        target=f"silver_{entity}",
        source=f"{entity}_valid",
        keys=[key],
        sequence_by=F.col("change_ts"),
        stored_as_scd_type=2,
        track_history_column_list=[*s.SCD2_ATTRS[entity], "is_deleted"],
        except_column_list=["op", "lsn", "_source_file", "_ingested_at"],
    )

# ---- orders: latest state (SCD1) sequenced by change time then LSN -------------
typed_view("orders_typed", "bronze_orders_cdc", s.prepare_orders)
quarantine_table("orders", "orders_typed", quality.ORDER_RULES)
valid_view("orders_valid", "orders_typed", quality.ORDER_RULES)
dlt.create_streaming_table(name="silver_orders", comment="Current state of each order.")
dlt.create_auto_cdc_flow(
    target="silver_orders",
    source="orders_valid",
    keys=["order_id"],
    sequence_by=F.struct("change_ts", "lsn"),  # late or duplicate events can never win
    stored_as_scd_type=1,
)


# ---- order items: row rules + referential check against the product master ----
def _product_check(df):
    known = dlt.read("silver_products").select("product_id").distinct().withColumn("_product_known", F.lit(True))
    return df.join(known, "product_id", "left").withColumn("_product_known", F.coalesce("_product_known", F.lit(False)))


ITEM_RULES = {**quality.ORDER_ITEM_RULES, "product_exists": "_product_known"}
typed_view("order_items_typed", "bronze_order_items", s.prepare_order_items, extra=_product_check)
quarantine_table("order_items", "order_items_typed", ITEM_RULES)
valid_view("order_items_valid", "order_items_typed", ITEM_RULES, drop_cols=("_raw",))

# ---- payments & inventory: typed + quality ---------------------------------------
for entity, rules, prep in [
    ("payments", quality.PAYMENT_RULES, s.prepare_payments),
    ("inventory", quality.INVENTORY_RULES, s.prepare_inventory),
]:
    typed_view(f"{entity}_typed", f"bronze_{entity}", prep)
    quarantine_table(entity, f"{entity}_typed", rules)
    valid_view(f"{entity}_valid", f"{entity}_typed", rules)

# ---- append-style sources deduplicated by key (SCD1 keeps one row per key) -----
for entity, keys in [
    ("order_items", ["order_id", "order_item_id"]),
    ("payments", ["order_id", "payment_sequential"]),
    ("inventory", ["supplier_id", "product_id", "warehouse_code", "snapshot_date"]),
]:
    dlt.create_streaming_table(name=f"silver_{entity}")
    dlt.create_auto_cdc_flow(
        target=f"silver_{entity}",
        source=f"{entity}_valid",
        keys=keys,
        sequence_by=F.col("_ingested_at"),
        stored_as_scd_type=1,
        except_column_list=["_product_known"] if entity == "order_items" else None,
    )

# ---- clickstream: streaming dedupe of at-least-once delivery ---------------------
typed_view("clickstream_typed", "bronze_clickstream", s.prepare_clickstream)
quarantine_table("clickstream", "clickstream_typed", quality.CLICK_RULES)


@dlt.table(
    name="silver_clickstream",
    comment="Clickstream deduplicated on event_id; events up to 2h late are accepted.",
    cluster_by=["event_date", "event_type"],
)
@dlt.expect_all_or_drop(quality.CLICK_RULES)
def silver_clickstream():
    return dlt.read_stream("clickstream_typed").drop("_raw").withWatermark("event_ts", "2 hours").dropDuplicatesWithinWatermark(["event_id"])


# =============================================================================
# Gold: materialized views
# =============================================================================


def scd2_dim(table: str, key: str, sk: str):
    """Map Lakeflow's __START_AT/__END_AT columns onto the package's dimension shape."""
    df = (
        dlt.read(table)
        .withColumnRenamed("__START_AT", "effective_from")
        .withColumnRenamed("__END_AT", "effective_to")
        .withColumn("is_current", F.col("effective_to").isNull())
    )
    return gold.build_dim(df, key, sk)


@dlt.table(name="gold_dim_customer", comment="Customer dimension (SCD2). Email is masked for non-PII readers via a view.")
def gold_dim_customer():
    return scd2_dim("silver_customers", "customer_id", "customer_sk")


@dlt.table(name="gold_dim_product", comment="Product dimension (SCD2).")
def gold_dim_product():
    return scd2_dim("silver_products", "product_id", "product_sk")


@dlt.table(name="gold_dim_date")
def gold_dim_date():
    return gold.build_dim_date(dlt.read("silver_orders"))


@dlt.table(name="gold_fact_order_items", comment="Grain: one row per order line; dimension keys resolved as of purchase time.")
@dlt.expect_or_fail("unique_line", "order_id IS NOT NULL AND order_item_id IS NOT NULL")
def gold_fact_order_items():
    return gold.build_fact_order_items(
        dlt.read("silver_order_items"), dlt.read("silver_orders"), dlt.read("gold_dim_customer"), dlt.read("gold_dim_product")
    )


@dlt.table(name="gold_fact_order_payments", comment="Grain: one row per order (payments aggregated before any join).")
def gold_fact_order_payments():
    return gold.build_fact_order_payments(dlt.read("silver_payments"))


@dlt.table(name="gold_recon_order_revenue", comment="Order revenue vs payments, with mismatch reasons.")
def gold_recon_order_revenue():
    q = dlt.read("silver_order_items_quarantine").select(F.col("_raw.order_id").alias("order_id")).unionByName(
        dlt.read("silver_payments_quarantine").select(F.col("_raw.order_id").alias("order_id"))
    )
    return gold.build_recon(dlt.read("gold_fact_order_items"), dlt.read("gold_fact_order_payments"), q.where("order_id IS NOT NULL"))


@dlt.table(name="gold_agg_daily_sales", cluster_by=["date_key"])
def gold_agg_daily_sales():
    return gold.build_daily_sales(dlt.read("gold_fact_order_items"), dlt.read("gold_dim_product"))


@dlt.table(name="gold_agg_inventory_status")
def gold_agg_inventory_status():
    return gold.build_inventory_status(dlt.read("silver_inventory"), dlt.read("gold_fact_order_items"), dlt.read("gold_dim_product"))


@dlt.table(name="gold_agg_funnel_daily")
def gold_agg_funnel_daily():
    return gold.build_funnel(dlt.read("silver_clickstream"))
