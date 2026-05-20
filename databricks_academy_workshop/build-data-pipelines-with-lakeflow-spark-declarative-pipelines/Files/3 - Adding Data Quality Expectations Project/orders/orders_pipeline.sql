----------------------------
-- ORDERS SPARK DECLARATIVE PIPELINE
-- Data Quality Expectations
----------------------------

------------------------------------------------------------------
-- JSON -> Bronze
------------------------------------------------------------------
-- Create the bronze streaming table in your labuser.1_bronze_db schema (database) and ingest the JSON files
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.orders_bronze_demo3
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
    "${source}/orders",  -- Uses the source configuration variable set in the pipeline settings
    format => 'JSON'
);


------------------------------------------------------------------
-- Bronze -> Silver (Contains Data Quality Expectations)
------------------------------------------------------------------
-- Create the silver streaming table in your labuser.2_silver_db schema (database) with data expectations
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver_demo3
  (
    -- Check for a 'Y' or 'x' in the notifications column, returns a warning
    CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','x')),
    -- Drop row if not a valid date (set to 2021-12-26)
    CONSTRAINT valid_date EXPECT (order_timestamp > "2021-12-26") ON VIOLATION DROP ROW,
    -- Fail pipeline if null
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications 
FROM STREAM sdp_1_bronze.orders_bronze_demo3; 


------------------------------------------------------------------
-- Gold Materialized View
------------------------------------------------------------------
-- Create the materialized view aggregation from the orders_silver_demo3 with the summarization
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.gold_orders_by_date_demo3 
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM sdp_2_silver.orders_silver_demo3   
GROUP BY date(order_timestamp);