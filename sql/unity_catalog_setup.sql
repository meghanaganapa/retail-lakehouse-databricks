-- =============================================================================
-- Unity Catalog setup: run once per environment as a metastore/catalog admin.
-- Replace retail_dev with retail_prod for production.
-- =============================================================================

-- 1. Catalog per environment, one schema for the lakehouse, one for landing files.
CREATE CATALOG IF NOT EXISTS retail_dev;
CREATE SCHEMA  IF NOT EXISTS retail_dev.retail  COMMENT 'Bronze, silver and gold tables (layer = table prefix)';
CREATE SCHEMA  IF NOT EXISTS retail_dev.landing COMMENT 'Raw files before ingestion';

-- 2. Landing zone. Option A: a managed volume (simplest).
CREATE VOLUME IF NOT EXISTS retail_dev.landing.raw;

-- Option B: files land in ADLS Gen2 and are read through an external location.
-- The storage credential uses the Access Connector's managed identity, so there
-- are no storage keys anywhere (see infra/setup_azure.sh).
-- CREATE STORAGE CREDENTIAL retail_mi WITH (AZURE_MANAGED_IDENTITY (ACCESS_CONNECTOR_ID = '<access-connector-resource-id>'));
-- CREATE EXTERNAL LOCATION retail_landing
--   URL 'abfss://landing@<storageaccount>.dfs.core.windows.net/'
--   WITH (STORAGE CREDENTIAL retail_mi);
-- CREATE EXTERNAL VOLUME retail_dev.landing.raw LOCATION 'abfss://landing@<storageaccount>.dfs.core.windows.net/raw';

-- 3. PII protection: analysts see gold through a view that masks email unless
--    they are in the pii_readers group. Raw silver stays engineer-only.
CREATE OR REPLACE VIEW retail_dev.retail.gold_v_dim_customer AS
SELECT
  customer_sk,
  customer_id,
  full_name,
  CASE
    WHEN is_account_group_member('pii_readers') THEN email
    ELSE concat(left(email, 1), '***@', split_part(email, '@', 2))
  END AS email,
  city,
  state,
  segment,
  is_deleted,
  effective_from,
  effective_to,
  is_current
FROM retail_dev.retail.gold_dim_customer;

-- 4. Least-privilege grants.
GRANT USE CATALOG ON CATALOG retail_dev TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA retail_dev.retail TO `data_engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME retail_dev.landing.raw TO `data_engineers`;

GRANT USE CATALOG ON CATALOG retail_dev TO `data_analysts`;
GRANT USE SCHEMA  ON SCHEMA  retail_dev.retail TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_fact_order_items     TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_fact_order_payments  TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_dim_product          TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_dim_date             TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_v_dim_customer       TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_agg_daily_sales      TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_agg_inventory_status TO `data_analysts`;
GRANT SELECT ON TABLE retail_dev.retail.gold_agg_funnel_daily     TO `data_analysts`;
-- Note: analysts get the masking view, never gold_dim_customer itself.
