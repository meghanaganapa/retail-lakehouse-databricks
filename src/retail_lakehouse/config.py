"""Runtime configuration and table naming.

Every table lives in one logical namespace with a layer prefix
(`bronze_orders_cdc`, `silver_orders`, `gold_fact_order_items`, ...). The same
names are used by the Lakeflow pipeline on Databricks, so the quality report can
run against either.

Two storage modes:

* ``path`` - tables are folders under ``base_path`` (local dev, tests, CI).
* ``uc``   - tables are Unity Catalog tables ``<catalog>.<schema>.<name>``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    landing_path: str = "./landing"
    base_path: str = "./lakehouse_data"
    storage: str = "path"  # "path" | "uc"
    fmt: str = "delta"  # "delta" | "parquet"; parquet is the local fallback
    catalog: str = "retail_dev"
    schema: str = "retail"

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        env = {
            "landing_path": os.getenv("LAKEHOUSE_LANDING"),
            "base_path": os.getenv("LAKEHOUSE_BASE_PATH"),
            "storage": os.getenv("LAKEHOUSE_STORAGE"),
            "fmt": os.getenv("LAKEHOUSE_FORMAT"),
            "catalog": os.getenv("LAKEHOUSE_CATALOG"),
            "schema": os.getenv("LAKEHOUSE_SCHEMA"),
        }
        values = {k: v for k, v in env.items() if v}
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)

    def table_id(self, name: str) -> str:
        """Fully qualified table name (uc) or folder path (path)."""
        if self.storage == "uc":
            return f"{self.catalog}.{self.schema}.{name}"
        return f"{self.base_path.rstrip('/')}/{name}"


# Business rules that live in config rather than code.
VALID_ORDER_STATUSES = ("created", "approved", "shipped", "delivered", "canceled")
VALID_EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
DEFAULT_WAREHOUSE = "MAIN"  # used for supplier files sent before warehouse_code existed
LOW_STOCK_DAYS_OF_COVER = 7.0
LOW_STOCK_MIN_UNITS = 5
RECON_TOLERANCE = 0.01  # dollars

# Thresholds the quality report enforces (job fails if breached).
MAX_QUARANTINE_RATE = 0.05
MAX_RECON_MISMATCH_RATE = 0.03
