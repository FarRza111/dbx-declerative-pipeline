-- Silver Layer: Cleaned and validated customers
-- Filters nulls, trims whitespace, adds data quality flag
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT "Cleaned and validated customer records"
CLUSTER BY (id)
AS
SELECT
  id,
  TRIM(name)                                    AS name,
  (id IS NOT NULL AND TRIM(name) IS NOT NULL)   AS is_valid,
  _ingested_at,
  current_timestamp()                           AS _processed_at
FROM STREAM(bronze_customers)
WHERE id IS NOT NULL
  AND TRIM(name) IS NOT NULL;
