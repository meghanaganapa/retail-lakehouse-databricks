from datetime import datetime

from pyspark.sql import functions as F

from retail_lakehouse.gold import UNKNOWN_SK, as_of_join, build_fact_order_payments, build_recon


def test_as_of_join_picks_version_valid_at_purchase_time(spark):
    dim = spark.createDataFrame(
        [
            ("P1", "sk_old", datetime(2026, 8, 1), datetime(2026, 9, 10)),
            ("P1", "sk_new", datetime(2026, 9, 10), None),
        ],
        "product_id string, product_sk string, effective_from timestamp, effective_to timestamp",
    )
    fact = spark.createDataFrame(
        [("O1", "P1", datetime(2026, 9, 5)), ("O2", "P1", datetime(2026, 9, 12)), ("O3", "P9", datetime(2026, 9, 12))],
        "order_id string, product_id string, ts timestamp",
    )
    out = {r.order_id: r.product_sk for r in as_of_join(fact, dim, "product_id", "ts", "product_sk").collect()}
    assert out == {"O1": "sk_old", "O2": "sk_new", "O3": UNKNOWN_SK}


def test_payments_are_aggregated_before_joining_to_avoid_fan_out(spark):
    items = spark.createDataFrame([("O1", 1, 30.0), ("O1", 2, 20.0)], "order_id string, order_item_id int, line_total double")
    payments = spark.createDataFrame(
        [("O1", 1, "voucher", 1, 10.0), ("O1", 2, "credit_card", 1, 40.0)],
        "order_id string, payment_sequential int, payment_type string, payment_installments int, payment_value double",
    )
    # The bug: joining raw rows multiplies revenue (2 lines x 2 payments = 4 rows).
    naive = items.join(payments, "order_id").agg(F.sum("line_total")).collect()[0][0]
    assert naive == 100.0

    per_order = build_fact_order_payments(payments)
    assert per_order.collect()[0].amount_paid == 50
    recon = build_recon(items, per_order, spark.createDataFrame([], "order_id string")).collect()[0]
    assert recon.amount_sold == 50 and not recon.is_mismatch


def test_recon_explains_mismatches(spark):
    items = spark.createDataFrame([("O1", 50.0), ("O2", 10.0)], "order_id string, line_total double")
    payments = spark.createDataFrame([("O1", 45.0), ("O2", 25.0)], "order_id string, amount_paid double")
    quarantined = spark.createDataFrame([("O2",)], "order_id string")
    out = {r.order_id: r.mismatch_reason for r in build_recon(items, payments, quarantined).collect()}
    assert out == {"O1": "amount differs", "O2": "lines or payments in quarantine"}
