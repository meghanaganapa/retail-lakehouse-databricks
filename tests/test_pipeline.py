"""End-to-end: two arrival batches, then a rerun with nothing new."""

from pyspark.sql import functions as F

from retail_lakehouse import pipeline
from retail_lakehouse.bronze import Source, ingest_source
from retail_lakehouse.checks import run_checks
from retail_lakehouse.generate import write_batch


def test_schema_drift_and_exactly_once_ingest(lakehouse, tmp_path):
    folder = tmp_path / "landing" / "inventory"
    folder.mkdir(parents=True)
    (folder / "a.csv").write_text("supplier_id,product_id,qty_on_hand\nS01,P1,5\n")
    src = Source("inventory", "csv")
    landing = str(tmp_path / "landing")

    first = ingest_source(lakehouse, landing, src, batch_id=1)
    assert first["rows"] == 1

    (folder / "b.csv").write_text("supplier_id,product_id,qty_on_hand,warehouse_code\nS01,P1,7,MEL1\n")
    second = ingest_source(lakehouse, landing, src, batch_id=2)
    assert second["files"] == 1  # a.csv is not read again
    assert second.get("new_columns") == ["warehouse_code"]

    bronze = lakehouse.read("bronze_inventory")
    assert bronze.count() == 2
    assert bronze.where("warehouse_code IS NULL").count() == 1  # old row kept, new column null


def test_two_batches_end_to_end_and_idempotent_rerun(lakehouse):
    landing = lakehouse.settings.landing_path

    write_batch(landing, 1)
    s1 = pipeline.run(lakehouse, landing)
    write_batch(landing, 2)
    s2 = pipeline.run(lakehouse, landing)
    assert s1["batch_id"] == 1 and s2["batch_id"] == 2

    fact_rows = lakehouse.read("gold_fact_order_items").count()
    dim_rows = lakehouse.read("gold_dim_customer").count()

    # Rerun with no new files: nothing should change.
    s3 = pipeline.run(lakehouse, landing)
    assert all(b["files"] == 0 for b in s3["bronze"])
    assert lakehouse.read("gold_fact_order_items").count() == fact_rows
    assert lakehouse.read("gold_dim_customer").count() == dim_rows

    # Late, stale "approved" events must not roll back delivered orders.
    orders = lakehouse.read("silver_orders")
    assert orders.where("status = 'delivered'").count() > 0
    bronze = lakehouse.read("bronze_orders_cdc").withColumn("lsn", F.col("lsn").cast("long"))
    newest = bronze.groupBy("order_id").agg(F.max_by("status", F.struct("change_ts", "lsn")).alias("expected"))
    assert orders.join(newest, "order_id").where("status != expected").count() == 0

    # Schema drift: batch 2 inventory files carry real warehouse codes.
    warehouses = {r.warehouse_code for r in lakehouse.read("silver_inventory").select("warehouse_code").distinct().collect()}
    assert warehouses == {"MAIN", "MEL1", "SYD1"}

    # Every bad order line was quarantined, none reached gold.
    fact = lakehouse.read("gold_fact_order_items")
    assert fact.where("unit_price <= 0 OR quantity <= 0 OR product_id = 'P999'").count() == 0
    assert lakehouse.read("silver_order_items_quarantine").count() > 0

    checks = run_checks(lakehouse)
    failed = [c.name for c in checks if not c.passed]
    assert failed == []
