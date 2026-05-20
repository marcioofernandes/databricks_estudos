------------------------------------------------------
-- CHANGE DATA CAPTURE WITH SCD TYPE 1 LAB
------------------------------------------------------


------------------------------------------------
-- A. CSV -> BRONZE STREAMING TABLE
------------------------------------------------
-- Adds the "pipelines.reset.allowed" = false property to prevent full refreshes on the initial ingestion layer
------------------------------------------------
-- REQUIREMENTS:
  -- Simply review the completed code below
------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_lab_1_bronze.employees_raw_bronze_demo7
  COMMENT "Raw ingestion from CSV files"
  TBLPROPERTIES (
      "quality" = "bronze", 
      "pipelines.reset.allowed" = false    -- prevent full table refreshes on the bronze table
  )             
AS 
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '${source}',
  format => 'CSV'
);


--------------------------------------------------------------
-- B. BRONZE STREAMING TABLE -> BRONZE CLEAN STREAMING TABLE
--------------------------------------------------------------
-- Add data quality constraints
-- Select the necessary columns and perform minor transformations
--------------------------------------------------------------
-- REQUIREMENTS:
  -- Simply review the completed code below.
--------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_lab_1_bronze.employees_bronze_clean_demo7
  (
    CONSTRAINT valid_emp_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_country EXPECT (Country IN ('US','GR'))
  )
  COMMENT "Clean the raw bronze table and prepare for CDC SCD Type 1"
  TBLPROPERTIES (
      "quality" = "bronze_cleaned_for_cdc" 
  )   
AS 
SELECT
  EmployeeID,
  FirstName,
  upper(Country) AS Country,
  Department,
  Salary,
  HireDate,
  Operation,
  ProcessDate
FROM STREAM sdp_lab_1_bronze.employees_raw_bronze_demo7;


--------------------------------------------------------------
-- C. BRONZE CLEAN STREAMING TABLE -> SILVER CDC STREAMING TABLE (SCD TYPE 1)
--------------------------------------------------------------
-- REQUIREMENTS:
  -- CREATE empty Streaming table named: `sdp_lab_2_silver.current_employees_silver_demo7`
  -- Complete the `AUTO CDC INTO` statement
  -- Use the `sdp_lab_2_silver.current_employees_silver_demo7` as the target
  -- Use the `sdp_lab_1_bronze.employees_bronze_clean_demo7` as the source
  -- Perform SCD Type 1 (the default)
  -- Delete all rows marked as 'delete'
  -- Select all columns except `operation`


--------------------------------------------------------------
-- TO DO: Add the AUTO CDC INTO STATEMENT BELOW. USE THE COMMENT AS A REFRENCE
--------------------------------------------------------------
-- -- Create the empty streaming table
-- <FILL-IN>


-- -- Perform CDC SCD Type 1
-- CREATE FLOW scd_type_1_flow AS 
-- AUTO CDC INTO<FILL-IN>  -- Target table to update with SCD Type 1 (or 2)
-- FROM STREAM <FILL-IN>        -- Source records to determine updates,
-- KEYS (<FILL-IN>)
-- APPLY AS DELETE WHEN <FILL-IN>
-- SEQUENCE BY <FILL-IN>
-- COLUMNS * EXCEPT (<FILL-IN>)
-- STORED AS <FILL-IN>;
