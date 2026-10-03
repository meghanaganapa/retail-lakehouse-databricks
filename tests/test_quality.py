from retail_lakehouse.quality import ORDER_ITEM_RULES, rule_failure_counts, split_valid_invalid
from retail_lakehouse.silver import prepare_order_items


def _bronze_items(spark, rows):
    cols = ["order_id", "order_item_id", "product_id", "quantity", "unit_price", "freight_value"]
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))
    from pyspark.sql import functions as F

    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("test.csv"))
        .withColumn("_batch_id", F.lit(1).cast("long"))
    )


def test_bad_rows_are_quarantined_with_reasons(spark):
    bronze = _bronze_items(
        spark,
        [
            ("O1", "1", "P1", "2", "10.00", "1.50"),  # good
            ("O2", "1", "P1", "1", "-5.00", "0"),  # negative price
            ("O3", "1", "P1", "0", "9.99", "0"),  # zero quantity
            ("O4", "1", "P1", "1", "N/A", "0"),  # unparseable price
            ("", "1", "P1", "1", "9.99", "0"),  # empty key
        ],
    )
    valid, invalid = split_valid_invalid(prepare_order_items(bronze), ORDER_ITEM_RULES)

    assert [r.order_id for r in valid.collect()] == ["O1"]
    assert invalid.count() == 4
    counts = rule_failure_counts(invalid)
    assert counts["price_positive"] >= 1
    assert counts["quantity_positive"] == 1
    assert counts["price_parsed"] == 1
    assert counts["order_id_not_null"] == 1


def test_quarantine_keeps_original_raw_values(spark):
    bronze = _bronze_items(spark, [("O4", "1", "P1", "1", "N/A", "0")])
    _, invalid = split_valid_invalid(prepare_order_items(bronze), ORDER_ITEM_RULES)
    row = invalid.collect()[0]
    assert row["_raw"]["unit_price"] == "N/A"  # what the source actually sent
    assert "_source_file" in invalid.columns  # lineage survives into quarantine


def test_null_result_counts_as_failure(spark):
    df = spark.createDataFrame([(None,)], "x int")
    valid, invalid = split_valid_invalid(df, {"x_positive": "x > 0"})
    assert valid.count() == 0 and invalid.count() == 1
