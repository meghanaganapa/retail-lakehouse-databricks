"""Static check of the Lakeflow pipeline definition.

The real pipeline only runs on Databricks, but we can still import it with a
stub ``dlt`` module and verify that it defines every table the quality report
and dashboards depend on, with the same names as the local pipeline.
"""

import runpy
import sys
import types
from pathlib import Path

from retail_lakehouse.checks import QUARANTINE_SOURCES

PIPELINE = Path(__file__).resolve().parents[1] / "pipelines" / "retail_pipeline.py"


def _stub_dlt(registry: dict):
    dlt = types.ModuleType("dlt")

    def table(name=None, **_):
        def deco(fn):
            registry["tables"].add(name or fn.__name__)
            return fn

        return deco

    def view(name=None, **_):
        def deco(fn):
            registry["views"].add(name or fn.__name__)
            return fn

        return deco

    def passthrough(*_a, **_k):
        return lambda fn: fn

    dlt.table, dlt.view = table, view
    dlt.expect_all_or_drop = dlt.expect_or_fail = dlt.expect_all = passthrough
    dlt.create_streaming_table = lambda name, **_: registry["tables"].add(name)
    dlt.create_auto_cdc_flow = lambda **kw: registry["cdc"].append(kw)
    return dlt


class _Conf:
    def get(self, key, default=None):
        return {"landing_path": "/Volumes/x/landing/raw", "bundle.sourcePath": "src"}.get(key, default)


def test_pipeline_defines_all_tables(monkeypatch, spark):  # spark: Column expressions need an active session
    registry = {"tables": set(), "views": set(), "cdc": []}
    monkeypatch.setitem(sys.modules, "dlt", _stub_dlt(registry))
    runpy.run_path(str(PIPELINE), init_globals={"spark": types.SimpleNamespace(conf=_Conf()), "dbutils": None})

    expected = {f"bronze_{s}" for s in QUARANTINE_SOURCES} | set(QUARANTINE_SOURCES.values())
    expected |= {
        "silver_customers", "silver_products", "silver_orders", "silver_order_items",
        "silver_payments", "silver_inventory", "silver_clickstream",
        "gold_dim_customer", "gold_dim_product", "gold_dim_date", "gold_fact_order_items",
        "gold_fact_order_payments", "gold_recon_order_revenue", "gold_agg_daily_sales",
        "gold_agg_inventory_status", "gold_agg_funnel_daily",
    }  # fmt: skip
    assert expected <= registry["tables"], expected - registry["tables"]

    scd2 = {c["target"] for c in registry["cdc"] if c["stored_as_scd_type"] == 2}
    assert scd2 == {"silver_customers", "silver_products"}
