-- =============================================================================
-- Queries behind the "Retail Sales & Inventory" AI/BI dashboard.
-- Run on a Databricks SQL warehouse (or point Power BI at the same gold tables).
-- =============================================================================

-- Daily revenue and orders (line chart)
SELECT d.date, SUM(s.revenue) AS revenue, SUM(s.orders) AS orders
FROM retail_dev.retail.gold_agg_daily_sales s
JOIN retail_dev.retail.gold_dim_date d USING (date_key)
GROUP BY d.date
ORDER BY d.date;

-- Revenue by category, last 7 days (bar chart)
SELECT category, SUM(revenue) AS revenue, SUM(units) AS units
FROM retail_dev.retail.gold_agg_daily_sales
WHERE date_key >= CAST(date_format(date_sub(current_date(), 7), 'yyyyMMdd') AS INT)
GROUP BY category
ORDER BY revenue DESC;

-- Products that need reordering (table)
SELECT product_id, product_name, category, supplier_id, qty_on_hand,
       avg_daily_units_7d, days_of_cover, stock_status
FROM retail_dev.retail.gold_agg_inventory_status
WHERE stock_status IN ('out_of_stock', 'low')
ORDER BY days_of_cover NULLS FIRST;

-- Conversion funnel (line chart)
SELECT event_date, sessions, cart_rate, conversion_rate
FROM retail_dev.retail.gold_agg_funnel_daily
ORDER BY event_date;

-- Revenue by customer state, reported at the address the customer had *when they ordered* (SCD2)
SELECT c.state, SUM(f.item_revenue) AS revenue
FROM retail_dev.retail.gold_fact_order_items f
JOIN retail_dev.retail.gold_v_dim_customer c USING (customer_sk)
WHERE f.order_status <> 'canceled'
GROUP BY c.state
ORDER BY revenue DESC;

-- -----------------------------------------------------------------------------
-- Ops dashboard
-- -----------------------------------------------------------------------------

-- Reconciliation: sold vs paid
SELECT coalesce(mismatch_reason, 'reconciled') AS status, COUNT(*) AS orders, SUM(difference) AS total_difference
FROM retail_dev.retail.gold_recon_order_revenue
GROUP BY 1;

-- Quarantine volume by rule
SELECT rule, COUNT(*) AS rows_failed
FROM (
  SELECT explode(_failed_rules) AS rule FROM retail_dev.retail.silver_order_items_quarantine
  UNION ALL
  SELECT explode(_failed_rules) FROM retail_dev.retail.silver_payments_quarantine
)
GROUP BY rule
ORDER BY rows_failed DESC;

-- Latest quality-gate results
SELECT check_name, value, threshold, passed, detail, checked_at
FROM retail_dev.retail.ops_quality_results
QUALIFY checked_at = MAX(checked_at) OVER ();

-- Freshness: minutes since the last file landed in each bronze table
SELECT 'orders' AS source, timestampdiff(MINUTE, MAX(_ingested_at), current_timestamp()) AS minutes_since_last_load
FROM retail_dev.retail.bronze_orders_cdc
UNION ALL
SELECT 'clickstream', timestampdiff(MINUTE, MAX(_ingested_at), current_timestamp()) FROM retail_dev.retail.bronze_clickstream;

-- Performance: silver_clickstream and gold_agg_daily_sales use liquid clustering
-- (cluster_by in pipelines/retail_pipeline.py). Time this before and after to measure the gain:
SELECT event_type, COUNT(*) FROM retail_dev.retail.silver_clickstream
WHERE event_date BETWEEN '2026-09-15' AND '2026-09-21'
GROUP BY event_type;
