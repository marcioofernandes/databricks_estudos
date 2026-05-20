--------------------------------------------------------
-- ORDERS PIPELINE
-- Adding COMMENTS & TBLPROPERTIES to objects
-- Otherwise all code is the same in this file as the previous demonstrations
--------------------------------------------------------
 

----------------------------------------------------------------------------------------------------------------
-- Create the bronze streaming table in your labuser.1_bronze_db schema (database) and incrementally ingest the JSON files
----------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.orders_bronze_demo5
  COMMENT "Ingest order JSON files from cloud storage"       -- Adds a comment to the table
  TBLPROPERTIES (
      "quality" = "bronze",                -- Adds a simple table property to the table
      "pipelines.reset.allowed" = false    -- prevent full table refreshes on the bronze table
  )             
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
    "${source}/orders",  -- Uses the source configuration variable set in the pipeline settings
    format => 'JSON'
);


----------------------------------------------------------------------------------------------------------------
-- Create the silver streaming table in your labuser.2_silver_db schema (database) with data expectations
----------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver_demo5
  (
    -- Check for a 'Y' or 'N' in the notifications column, returns a warning
    CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','N')),
    -- Drop row if not a valid date (set to 2021-01-01)
    CONSTRAINT valid_date EXPECT (order_timestamp > "2021-12-25") ON VIOLATION DROP ROW,
    -- Fail pipeline if null
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT "Silver clean orders table"   -- Adds a comment to the table
  TBLPROPERTIES ("quality" = "silver")         
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM sdp_1_bronze.orders_bronze_demo5; 


----------------------------------------------------------------------------------------------------------------
-- Create the materialized view aggregation from the orders_silver table with the summarization
----------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.gold_orders_by_date_demo5 
  COMMENT "Aggregate gold data for downstream analysis"        -- Adds a comment to the table
  TBLPROPERTIES ("quality" = "gold")                           -- Adds a simple table property to the table
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM sdp_2_silver.orders_silver_demo5  
GROUP BY date(order_timestamp);