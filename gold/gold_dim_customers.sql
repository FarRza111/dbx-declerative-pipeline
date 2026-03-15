-- Gold Layer: Business-ready customer dimension table
-- Denormalized, read-optimized for BI tools and dashboards
CREATE OR REFRESH MATERIALIZED VIEW gold_dim_customers
COMMENT "Business-ready customer dimension table"
CLUSTER BY (customer_id)
AS
SELECT
  id                                  AS customer_id,
  name                                AS customer_name,
  UPPER(SUBSTRING(name, 1, 1))        AS name_initial,
  _ingested_at,
  _processed_at,
  current_timestamp()                 AS _enriched_at
FROM silver_customers
WHERE is_valid = true;
