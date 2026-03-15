-- Bronze Layer: Raw customer ingestion from raw.jaffle_shop_raw.raw_customers
-- Minimal transformation - preserves source data with audit metadata
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw customer data ingested from raw.jaffle_shop_raw.raw_customers"
CLUSTER BY (id)
AS
SELECT
  ID   AS id,
  NAME AS name,
  current_timestamp() AS _ingested_at
FROM STREAM(raw.jaffle_shop_raw.raw_customers);
