from datetime import datetime

from pyspark.sql import functions as F

from retail_lakehouse.silver import apply_scd2, dedupe_latest

SCHEMA = "customer_id string, city string, op string, change_ts timestamp, lsn long"
ATTRS = ["city"]


def ts(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 9, day, hour)


def empty_dim(spark):
    return spark.createDataFrame(
        [],
        "customer_id string, city string, is_deleted boolean, effective_from timestamp, effective_to timestamp, "
        "is_current boolean, _lsn long, _row_hash string",
    )


def history(dim, key="C1"):
    return [
        (r.city, r.effective_from, r.effective_to, r.is_current)
        for r in dim.where(F.col("customer_id") == key).orderBy("effective_from").collect()
    ]


# ------------------------------------------------------------------ dedupe


def test_dedupe_keeps_latest_event_even_when_it_arrives_first(spark):
    events = spark.createDataFrame(
        [
            ("O1", "delivered", ts(5), 30),  # arrives first in the file, but is the newest
            ("O1", "created", ts(1), 10),
            ("O1", "approved", ts(2), 20),
            ("O1", "approved", ts(2), 20),  # exact duplicate re-sent by the source
        ],
        "order_id string, status string, change_ts timestamp, lsn long",
    )
    latest = dedupe_latest(events, ["order_id"], [F.col("change_ts").desc(), F.col("lsn").desc()])
    assert [r.status for r in latest.collect()] == ["delivered"]


def test_lsn_breaks_ties_on_identical_timestamps(spark):
    events = spark.createDataFrame(
        [("O1", "approved", ts(2), 20), ("O1", "shipped", ts(2), 21)],
        "order_id string, status string, change_ts timestamp, lsn long",
    )
    latest = dedupe_latest(events, ["order_id"], [F.col("change_ts").desc(), F.col("lsn").desc()])
    assert latest.collect()[0].status == "shipped"


# ------------------------------------------------------------------ SCD2


def test_scd2_builds_history(spark):
    changes = spark.createDataFrame(
        [("C1", "Melbourne", "I", ts(1), 1), ("C1", "Sydney", "U", ts(10), 2), ("C2", "Perth", "I", ts(1), 3)], SCHEMA
    )
    dim = apply_scd2(empty_dim(spark), changes, "customer_id", ATTRS)
    assert history(dim) == [("Melbourne", ts(1), ts(10), False), ("Sydney", ts(10), None, True)]
    assert dim.where("is_current").count() == 2


def test_scd2_late_event_slots_into_the_middle_of_history(spark):
    batch1 = spark.createDataFrame([("C1", "Melbourne", "I", ts(1), 1), ("C1", "Sydney", "U", ts(20), 3)], SCHEMA)
    dim = apply_scd2(empty_dim(spark), batch1, "customer_id", ATTRS)
    # An update that happened on day 10 only arrives now.
    late = spark.createDataFrame([("C1", "Brisbane", "U", ts(10), 2)], SCHEMA)
    dim = apply_scd2(dim, late, "customer_id", ATTRS)
    assert history(dim) == [
        ("Melbourne", ts(1), ts(10), False),
        ("Brisbane", ts(10), ts(20), False),
        ("Sydney", ts(20), None, True),
    ]


def test_scd2_is_idempotent_and_ignores_duplicates(spark):
    changes = spark.createDataFrame(
        [("C1", "Melbourne", "I", ts(1), 1), ("C1", "Sydney", "U", ts(10), 2), ("C1", "Sydney", "U", ts(10), 2)], SCHEMA
    )
    once = apply_scd2(empty_dim(spark), changes, "customer_id", ATTRS)
    twice = apply_scd2(once, changes, "customer_id", ATTRS)
    assert history(once) == history(twice)
    assert twice.count() == 2


def test_scd2_collapses_no_op_updates(spark):
    changes = spark.createDataFrame([("C1", "Melbourne", "I", ts(1), 1), ("C1", "Melbourne", "U", ts(5), 2)], SCHEMA)
    dim = apply_scd2(empty_dim(spark), changes, "customer_id", ATTRS)
    assert history(dim) == [("Melbourne", ts(1), None, True)]


def test_scd2_delete_is_a_soft_delete_version(spark):
    changes = spark.createDataFrame([("C1", "Melbourne", "I", ts(1), 1), ("C1", "Melbourne", "D", ts(7), 2)], SCHEMA)
    dim = apply_scd2(empty_dim(spark), changes, "customer_id", ATTRS)
    current = dim.where("is_current").collect()
    assert len(current) == 1 and current[0].is_deleted is True
    assert dim.count() == 2  # history before the delete is preserved


def test_scd2_leaves_unaffected_keys_untouched(spark):
    base = spark.createDataFrame([("C1", "Melbourne", "I", ts(1), 1), ("C2", "Perth", "I", ts(1), 2)], SCHEMA)
    dim = apply_scd2(empty_dim(spark), base, "customer_id", ATTRS)
    upd = spark.createDataFrame([("C1", "Hobart", "U", ts(3), 3)], SCHEMA)
    dim2 = apply_scd2(dim, upd, "customer_id", ATTRS)
    assert history(dim2, "C2") == history(dim, "C2")
