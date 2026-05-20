-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![Databricks Academy](./Includes/images/common/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6 - Change Data Capture with AUTO CDC with Slowly Changing Dimensions (SCD) TYPE 1
-- MAGIC
-- MAGIC In this demonstration, we will continue to build our pipeline by ingesting **customer** data. The customer data includes new customers, customers who have deleted their accounts, and customers who have updated their information (such as address, email, etc.). We will need to build our customer pipeline by implementing change data capture (CDC) for customer data using SCD Type 1 (Type 2 is outside the scope of this course).
-- MAGIC
-- MAGIC The customer pipeline flow:
-- MAGIC
-- MAGIC - The bronze table uses **Auto Loader** to ingest JSON data from cloud object storage with SQL (`FROM STREAM`).
-- MAGIC - A table is defined to enforce constraints before passing records to the silver layer.
-- MAGIC - `AUTO CDC` is used to automatically process CDC data into the silver layer as a Type 1.
-- MAGIC - A gold table is defined to create a materialized view of the current customers with updated information (dropped customers, new customers and updated customer information).
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Apply the `AUTO CDC` operation in Lakeflow Spark Declarative Pipelines to process change data capture (CDC) by integrating and updating incoming data from a source stream into an existing Delta table, ensuring data accuracy and consistency.
-- MAGIC - Analyze Slowly Changing Dimensions (SCD Type 1) tables within Lakeflow Spark Declarative Pipelines to effectively update, insert, and drop customers in dimensional data, managing the state of records over time using appropriate keys, versioning, and timestamps.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #7b1fa2;
-- MAGIC   background: #f3e5f5;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.1em;">Syntax Update</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC The AUTO CDC APIs replace the APPLY CHANGES APIs, and have the same syntax. The APPLY CHANGES APIs are still available, but Databricks recommends using the AUTO CDC APIs in their place.
-- MAGIC
-- MAGIC The AUTO CDC APIs - Simplify change data capture with pipelines:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/cdc) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/cdc)
-- MAGIC
-- MAGIC
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #f44336;
-- MAGIC   background: #ffebee;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Before starting this notebook, select the required compute environment listed below.
-- MAGIC
-- MAGIC - **Serverless Compute, Version 5**  
-- MAGIC ![Serverless Select](./Includes/images/common/select-serverless.png)
-- MAGIC <br></br>
-- MAGIC   - How to select an environment version:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
-- MAGIC
-- MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC 1. Run the following cell to configure your working environment for this course.
-- MAGIC
-- MAGIC     This cell will also reset your `/Volumes/labuser/sdp_1_bronze/source` volume with the JSON files to the starting point, with one JSON file in each directory.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-REQUIRED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore the Customer Data Source Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/labuser/sdp_1_bronze/source/customers` volume. 
-- MAGIC
-- MAGIC     Confirm you only see one **00.json** file for customers.

-- COMMAND ----------

-- DBTITLE 1,View files in the customers volume
-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{source_volume_path}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below to explore the customers **00.json** file located at `/Volumes/labuser/sdp_1_bronze/source/customers`. Note the following:
-- MAGIC
-- MAGIC    a. The file contains **939 customers** (remember this number).
-- MAGIC
-- MAGIC    b. It includes general customer information such as **email**, **name**, and **address**.
-- MAGIC
-- MAGIC    c. The **timestamp** column specifies the logical order of customer events in the source data.
-- MAGIC
-- MAGIC    d. The **operation** column indicates whether the entry is for a new customer, a deletion, or an update.
-- MAGIC       - **NOTE:** Since this is the first JSON file, all rows will be considered new customers.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Explore the customers raw JSON data
SELECT *
FROM read_files(
  source_volume_path || '/customers/00.json',
  format => "JSON"
)
ORDER BY operation;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #7b1fa2;
-- MAGIC   background: #f3e5f5;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.1em;">Question</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC How can we ingest new raw data source files (JSON) with customer updates into our pipeline to update the **customers_silver** table when inserts, updates, or deletes occur, without maintaining historical records (SCD Type 1)?
-- MAGIC
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Change Data Capture with AUTO CDC APIs in Lakeflow Spark Declarative Pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to create your starter Spark Declarative Pipeline for this demonstration. The pipeline will set the following for you:
-- MAGIC     - Your default catalog: **labuser**
-- MAGIC     - Your configuration parameter: `source` = `/Volumes/labuser/sdp_1_bronze/source/customers`
-- MAGIC
-- MAGIC     **NOTES:** 
-- MAGIC     - The `create_declarative_pipeline` function is a custom function built for this course to create the sample pipeline using the Databricks REST API. This avoids manually creating the pipeline and referencing the pipeline assets.
-- MAGIC
-- MAGIC     - If the pipeline already exists, an error will be returned. In that case, you'll need to delete the existing pipeline and rerun this cell.

-- COMMAND ----------

-- DBTITLE 1,Create pipeline 6
-- MAGIC %python
-- MAGIC create_declarative_pipeline(
-- MAGIC     pipeline_name=f'6 - Change Data Capture with AUTO CDC - {my_catalog}',
-- MAGIC     root_path_folder_name="6 - Change Data Capture with AUTO CDC Project",
-- MAGIC     catalog_name=my_catalog,
-- MAGIC     schema_name='default',
-- MAGIC     source_folder_names=['orders', 'status', 'customers'],
-- MAGIC     configuration={'source': source_volume_path}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to open the starter Spark Declarative Pipeline project for this demonstration:
-- MAGIC
-- MAGIC    a. In the main navigation bar, right-click on **Jobs & Pipelines** and select **Open Link in New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select your **6 - Change Data Capture with AUTO CDC - labuser** pipeline.
-- MAGIC       - **REQUIRED:** At the top near your pipeline name, turn on **New pipeline monitoring**.
-- MAGIC
-- MAGIC    c. In the **Pipeline details** pane on the far right, select **Open in Editor** (field to the right of **Source code**) to open the pipeline in the **Lakeflow Pipeline Editor**.
-- MAGIC
-- MAGIC    d. In the new tab, you should see the following folders:
-- MAGIC       - **explorations**
-- MAGIC       - **orders**
-- MAGIC       - **status**
-- MAGIC       - **customers**
-- MAGIC       - Plus the extra **python_excluded** folder that contains the Python version.
-- MAGIC
-- MAGIC    e. Open the **customers** folder and select the **customers_pipeline.sql** file.
-- MAGIC       - **NOTE:** The **status** and **orders** pipelines are the same as we saw in the previous demonstrations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Spark Declarative Pipeline CDC SCD Type 1 Pipeline Steps
-- MAGIC Follow the steps below using the **customers_pipeline.sql** file in the Lakeflow Pipelines editor.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below and confirm each source volume (for **orders**, **status** and **customers**) contains a single JSON file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{source_volume_path}/orders"').display()
-- MAGIC spark.sql(f'LIST "{source_volume_path}/status"').display()
-- MAGIC spark.sql(f'LIST "{source_volume_path}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. PLEASE COMPLETE FIRST: Click the 'Run Pipeline' button to execute the Pipeline
-- MAGIC 1. To save some time, let's run the entire pipeline for **status**, **orders** and **customers**. Each volume contains 1 file.
-- MAGIC
-- MAGIC     While the pipeline is running explore the code in the **customers/customers_pipeline.sql** for the new customers flow.
-- MAGIC
-- MAGIC ##### While the pipeline is running continue through the steps below to review the customer pipeline code.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. STEP 1: JSON -> Bronze Ingestion (`customers_pipeline.sql`)
-- MAGIC The code in **STEP 1** of the **customers_pipeline.sql** file:
-- MAGIC
-- MAGIC    - We define a bronze streaming table named **customers_bronze_raw_demo6** using a data source configured with Auto Loader (`FROM STREAM`) to incrementally ingest files from cloud storage.
-- MAGIC
-- MAGIC    - Adds the table property `pipelines.reset.allowed = false` to prevent deletion of all ingested bronze data if a full refresh is triggered.
-- MAGIC    
-- MAGIC    - Creates columns to capture the time of data ingestion and the source file name for each row.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### D3. STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement
-- MAGIC
-- MAGIC ##### **NOTE:** This displays how you can use advanced data quality techniques with expectations. Advanced expectations are outside the scope of this course.
-- MAGIC
-- MAGIC ##### The code in **STEP 2** of the **customers_pipeline.sql** file:
-- MAGIC
-- MAGIC - Adds three violation constraint actions: **WARN**, **DROP**, and **FAIL**. Each defines how to handle constraint violations.
-- MAGIC - Applies multiple conditions to a single constraint.
-- MAGIC - Uses a built-in SQL function within a constraint.
-- MAGIC
-- MAGIC ##### About the data source:
-- MAGIC
-- MAGIC - The data is a CDC feed that contains **`INSERT`**, **`UPDATE`**, and **`DELETE`** operations for customers.
-- MAGIC
-- MAGIC   - REQUIREMENT: **UPDATE** and **INSERT** operations should contain valid entries for all fields.
-- MAGIC
-- MAGIC   - REQUIREMENT: **DELETE** operations should contain **`NULL`** values for all fields except the **timestamp**, **customer_id**, and **operation** fields.
-- MAGIC
-- MAGIC   - When a record is going to be dropped, all values except the **customer_id** will be `null`.
-- MAGIC
-- MAGIC     <div style="max-width: 1100px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
-- MAGIC       <table style="width: 100%; border-collapse: collapse; font-size: 14pt; line-height: 1.5;">
-- MAGIC         <thead>
-- MAGIC           <tr style="background: #1B5162; color: white;">
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">address</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">city</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">customer_id</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">email</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">name</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">operation</th>
-- MAGIC             <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">state</th>
-- MAGIC           </tr>
-- MAGIC         </thead>
-- MAGIC         <tbody>
-- MAGIC           <tr style="background: #F9F7F4;">
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; color: #618794; font-style: italic;">null</td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; color: #618794; font-style: italic;">null</td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700; color: #0b2026;">23617</td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; color: #618794; font-style: italic;">null</td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; color: #618794; font-style: italic;">null</td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">
-- MAGIC               <span style="background: #FABFBA; color: #801C17; font-weight: 700; padding: 3px 10px; border-radius: 4px; font-size: 13pt;">DELETE</span>
-- MAGIC             </td>
-- MAGIC             <td style="padding: 10px 14px; border: 1px solid #EEEDE9; color: #618794; font-style: italic;">null</td>
-- MAGIC           </tr>
-- MAGIC         </tbody>
-- MAGIC       </table>
-- MAGIC
-- MAGIC     </div>
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **NOTE:** To ensure only valid data reaches our silver table, we'll write a series of quality enforcement rules that allow expected null values in **DELETE** operations while rejecting bad data elsewhere.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### We'll break down each of these constraints below:
-- MAGIC
-- MAGIC
-- MAGIC <div style="max-width: 1060px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
-- MAGIC
-- MAGIC <style>
-- MAGIC .f1-cards { display: flex; gap: 8px; margin-bottom: 18px; flex-wrap: wrap; }
-- MAGIC .f1-card {
-- MAGIC   flex: 1; background: #F9F7F4; border-top: 6px solid transparent;
-- MAGIC   border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent;
-- MAGIC   border-radius: 8px; padding: 12px 10px; text-align: center; cursor: pointer; user-select: none;
-- MAGIC   transition: transform 0.12s, background 0.15s; min-width: 140px;
-- MAGIC }
-- MAGIC .f1-card:hover { transform: translateY(-2px); }
-- MAGIC .f1-card.active { background: #fff; border-left-color: var(--dc); border-right-color: var(--dc); border-bottom-color: var(--dc); }
-- MAGIC .f1-card-label { display: block; font-size: 13pt; font-weight: 700; color: #0b2026; line-height: 1.3; pointer-events: none; }
-- MAGIC .f1-card-sub { display: block; font-size: 11.5pt; font-weight: 400; color: #618794; margin-top: 4px; pointer-events: none; }
-- MAGIC .f1-layout { display: flex; gap: 22px; align-items: stretch; }
-- MAGIC .f1-code-wrap { flex: 1; position: relative; }
-- MAGIC .f1-theme-btn {
-- MAGIC   position: absolute; top: 10px; right: 12px; z-index: 2;
-- MAGIC   background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.2);
-- MAGIC   border-radius: 6px; padding: 5px 12px; font-size: 11pt; font-weight: 600; color: #cdd6f4;
-- MAGIC   cursor: pointer; transition: background 0.15s, color 0.15s, border-color 0.15s;
-- MAGIC }
-- MAGIC .f1-theme-btn:hover { background: rgba(255,255,255,0.2); }
-- MAGIC .f1-code-wrap.light .f1-theme-btn { background: rgba(0,0,0,0.06); border-color: rgba(0,0,0,0.15); color: #444; }
-- MAGIC .f1-code-wrap.light .f1-theme-btn:hover { background: rgba(0,0,0,0.1); }
-- MAGIC .f1-code {
-- MAGIC   border-radius: 10px; padding: 20px 22px; font-family: 'Menlo','Consolas',monospace;
-- MAGIC   font-size: 12pt; line-height: 1.75; overflow-x: auto; background: #1e1e2e; color: #cdd6f4;
-- MAGIC   transition: background 0.3s, color 0.3s;
-- MAGIC }
-- MAGIC .f1-code-wrap.light .f1-code { background: #fafafa; color: #383a42; }
-- MAGIC .f1-code .tk-kw { color: #cba6f7; }
-- MAGIC .f1-code .tk-fn { color: #89b4fa; }
-- MAGIC .f1-code .tk-str { color: #a6e3a1; }
-- MAGIC .f1-code .tk-num { color: #fab387; }
-- MAGIC .f1-code .tk-cmt { color: #6c7086; }
-- MAGIC .f1-code .tk-dim { color: #a6adc8; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-kw { color: #a626a4; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-fn { color: #4078f2; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-str { color: #50a14f; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-num { color: #986801; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-cmt { color: #a0a1a7; }
-- MAGIC .f1-code-wrap.light .f1-code .tk-dim { color: #696c77; }
-- MAGIC .f1-code .line { display: block; padding: 1px 6px; border-radius: 3px; transition: background 0.25s, opacity 0.25s; }
-- MAGIC .f1-code.has-highlight .line { opacity: 0.25; }
-- MAGIC .f1-code.has-highlight .line.hl { opacity: 1; background: rgba(255,255,255,0.08); }
-- MAGIC .f1-code-wrap.light .f1-code.has-highlight .line.hl { background: rgba(0,0,0,0.06); }
-- MAGIC .f1-explain { flex: 0 0 320px; display: flex; flex-direction: column; justify-content: center; }
-- MAGIC .f1-explain-card { background: #F9F7F4; border-radius: 10px; border-top: 6px solid #ccc; padding: 20px; font-size: 14pt; line-height: 1.6; opacity: 0; transition: opacity 0.3s; min-height: 200px; }
-- MAGIC .f1-explain-card.visible { opacity: 1; }
-- MAGIC .f1-explain-card ul { margin: 10px 0 0 0; padding-left: 20px; }
-- MAGIC .f1-explain-card li { margin-bottom: 10px; }
-- MAGIC .f1-badge {
-- MAGIC   display: inline-block; padding: 3px 10px; border-radius: 999px;
-- MAGIC   font-size: 11.5pt; font-weight: 700; margin-bottom: 12px;
-- MAGIC }
-- MAGIC </style>
-- MAGIC
-- MAGIC <!-- Header -->
-- MAGIC <div style="background: #1B5162; color: white; border-radius: 8px 8px 4px 4px; padding: 20px 24px; margin-bottom: 18px;">
-- MAGIC   <div style="font-size: 20pt; font-weight: 700;">Data Quality Constraints - Bronze Clean Table</div>
-- MAGIC   <div style="font-size: 14pt; margin-top: 6px; opacity: 0.9;">Each constraint defines a rule and what happens when a record violates it. Click a constraint to explore.</div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div class="f1-cards">
-- MAGIC   <div class="f1-card" data-id="0" onclick="bcSelect(0)" style="--dc:#98102A; border-top-color:#98102A;">
-- MAGIC     <span class="f1-card-label"><code>valid_id</code></span>
-- MAGIC     <span class="f1-card-sub">FAIL UPDATE</span>
-- MAGIC   </div><div class="f1-card" data-id="1" onclick="bcSelect(1)" style="--dc:#FF5F46; border-top-color:#FF5F46;">
-- MAGIC     <span class="f1-card-label"><code>valid_operation</code></span>
-- MAGIC     <span class="f1-card-sub">DROP ROW</span>
-- MAGIC   </div><div class="f1-card" data-id="2" onclick="bcSelect(2)" style="--dc:#FFAB00; border-top-color:#FFAB00;">
-- MAGIC     <span class="f1-card-label"><code>valid_name</code></span>
-- MAGIC     <span class="f1-card-sub">WARN (default)</span>
-- MAGIC   </div><div class="f1-card" data-id="3" onclick="bcSelect(3)" style="--dc:#4299E0; border-top-color:#4299E0;">
-- MAGIC     <span class="f1-card-label"><code>valid_address</code></span>
-- MAGIC     <span class="f1-card-sub">WARN (default)</span>
-- MAGIC   </div><div class="f1-card" data-id="4" onclick="bcSelect(4)" style="--dc:#00A972; border-top-color:#00A972;">
-- MAGIC     <span class="f1-card-label"><code>valid_email</code></span>
-- MAGIC     <span class="f1-card-sub">DROP ROW</span>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div class="f1-layout">
-- MAGIC   <div class="f1-code-wrap" id="bc-code-wrap">
-- MAGIC     <button class="f1-theme-btn" id="bc-theme-btn" onclick="bcToggle()">Light Mode</button>
-- MAGIC     <div class="f1-code" id="bc-code">
-- MAGIC       <span class="line" data-g="all"><span class="tk-kw">CREATE STREAMING TABLE</span> 1_bronze_db.customers_bronze_clean_demo6</span>
-- MAGIC       <span class="line" data-g="all">&nbsp;&nbsp;(</span>
-- MAGIC       <span class="line" data-g="0">&nbsp;&nbsp;&nbsp;&nbsp;<span class="tk-kw">CONSTRAINT</span> <span class="tk-fn">valid_id</span> <span class="tk-kw">EXPECT</span> (customer_id <span class="tk-kw">IS NOT NULL</span>) <span class="tk-kw">ON VIOLATION FAIL UPDATE</span>,</span>
-- MAGIC       <span class="line" data-g="1">&nbsp;&nbsp;&nbsp;&nbsp;<span class="tk-kw">CONSTRAINT</span> <span class="tk-fn">valid_operation</span> <span class="tk-kw">EXPECT</span> (operation <span class="tk-kw">IS NOT NULL</span>) <span class="tk-kw">ON VIOLATION DROP ROW</span>,</span>
-- MAGIC       <span class="line" data-g="2">&nbsp;&nbsp;&nbsp;&nbsp;<span class="tk-kw">CONSTRAINT</span> <span class="tk-fn">valid_name</span> <span class="tk-kw">EXPECT</span> (name <span class="tk-kw">IS NOT NULL OR</span> operation = <span class="tk-str">"DELETE"</span>),</span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;<span class="tk-kw">CONSTRAINT</span> <span class="tk-fn">valid_address</span> <span class="tk-kw">EXPECT</span> (</span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(address <span class="tk-kw">IS NOT NULL AND</span></span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;city <span class="tk-kw">IS NOT NULL AND</span></span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;state <span class="tk-kw">IS NOT NULL AND</span></span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;zip_code <span class="tk-kw">IS NOT NULL</span>) <span class="tk-kw">OR</span></span>
-- MAGIC       <span class="line" data-g="3">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;operation = <span class="tk-str">"DELETE"</span>),</span>
-- MAGIC       <span class="line" data-g="4">&nbsp;&nbsp;&nbsp;&nbsp;<span class="tk-kw">CONSTRAINT</span> <span class="tk-fn">valid_email</span> <span class="tk-kw">EXPECT</span> (</span>
-- MAGIC       <span class="line" data-g="4">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;rlike(email, <span class="tk-str">'^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$'</span>) <span class="tk-kw">OR</span></span>
-- MAGIC       <span class="line" data-g="4">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;operation = <span class="tk-str">"DELETE"</span>) <span class="tk-kw">ON VIOLATION DROP ROW</span></span>
-- MAGIC       <span class="line" data-g="all">&nbsp;&nbsp;)</span>
-- MAGIC       <span class="line" data-g="all">&nbsp;&nbsp;<span class="tk-kw">COMMENT</span> <span class="tk-str">"Clean raw bronze timestamp column and add data quality constraints"</span></span>
-- MAGIC       <span class="line" data-g="all"><span class="tk-kw">AS</span></span>
-- MAGIC       <span class="line" data-g="all"><span class="tk-kw">SELECT</span></span>
-- MAGIC       <span class="line" data-g="all">&nbsp;&nbsp;*,</span>
-- MAGIC       <span class="line" data-g="all">&nbsp;&nbsp;<span class="tk-fn">CAST</span>(from_unixtime(timestamp) <span class="tk-kw">AS</span> timestamp) <span class="tk-kw">AS</span> timestamp_datetime</span>
-- MAGIC       <span class="line" data-g="all"><span class="tk-kw">FROM STREAM</span> 1_bronze_db.customers_bronze_raw_demo6;</span>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div class="f1-explain">
-- MAGIC     <div class="f1-explain-card" id="bc-explain-card"></div>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC </div>
-- MAGIC
-- MAGIC <script>
-- MAGIC var BC_DATA = [
-- MAGIC   {
-- MAGIC     color: '#98102A',
-- MAGIC     badge: 'FAIL UPDATE',
-- MAGIC     badgeBg: 'rgba(152,16,42,0.12)',
-- MAGIC     title: 'valid_id',
-- MAGIC     text: 'The strictest violation action. If any record arrives with a <strong>null <code>customer_id</code></strong>, the entire micro-batch update fails.',
-- MAGIC     bullets: [
-- MAGIC       'No records from that batch are written',
-- MAGIC       'Use for fields that are truly non-negotiable, like a primary key',
-- MAGIC       'Prefer this when bad data should halt the pipeline, not silently pass through'
-- MAGIC     ]
-- MAGIC   },
-- MAGIC   {
-- MAGIC     color: '#FF5F46',
-- MAGIC     badge: 'DROP ROW',
-- MAGIC     badgeBg: 'rgba(255,95,70,0.12)',
-- MAGIC     title: 'valid_operation',
-- MAGIC     text:       'Silently removes any record where <code>operation</code> is null. The pipeline keeps running and only the offending row is discarded.',
-- MAGIC     bullets: [
-- MAGIC       'Violation counts are tracked in pipeline metrics',
-- MAGIC       'Use when bad rows should be excluded but not stop processing',
-- MAGIC       'A null <code>operation</code> means we can\'t apply CDC logic — so there\'s no safe way to handle it'
-- MAGIC     ]
-- MAGIC   },
-- MAGIC   {
-- MAGIC     color: '#FFAB00',
-- MAGIC     badge: 'WARN (default)',
-- MAGIC     badgeBg: 'rgba(255,171,0,0.12)',
-- MAGIC     title: 'valid_name',
-- MAGIC     text: 'Flags records where <code>name</code> is null and the operation is not a DELETE. No rows are dropped — violations are tracked in metrics only.',
-- MAGIC     bullets: [
-- MAGIC       'No <code>ON VIOLATION</code> clause = WARN behavior by default',
-- MAGIC       'The <code>OR operation = "DELETE"</code> allows expected nulls for deletes',
-- MAGIC       'Use when you want visibility into data quality issues without disrupting the pipeline'
-- MAGIC     ]
-- MAGIC   },
-- MAGIC   {
-- MAGIC     color: '#4299E0',
-- MAGIC     badge: 'WARN (default)',
-- MAGIC     badgeBg: 'rgba(66,153,224,0.12)',
-- MAGIC     title: 'valid_address',
-- MAGIC     text: 'Checks all four address fields at once. A record passes if all four are non-null, OR if the operation is DELETE (where nulls are expected).',
-- MAGIC     bullets: [
-- MAGIC       'Multiple conditions combined with AND in a single constraint',
-- MAGIC       'The OR short-circuit lets DELETE records bypass address validation',
-- MAGIC       'Violations are logged to metrics but rows are not dropped'
-- MAGIC     ]
-- MAGIC   },
-- MAGIC   {
-- MAGIC     color: '#00A972',
-- MAGIC     badge: 'DROP ROW',
-- MAGIC     badgeBg: 'rgba(0,169,114,0.12)',
-- MAGIC     title: 'valid_email',
-- MAGIC     text: 'Uses <code>rlike()</code>, a built-in SQL regex function to validate email format. Records with an invalid email are dropped.',
-- MAGIC     bullets: [
-- MAGIC       'The regex pattern matches standard email formats',
-- MAGIC       'DELETE operations are exempt (their email field will be null)',
-- MAGIC       'Dropped records will have all fields null except <code>customer_id</code>'
-- MAGIC     ]
-- MAGIC   }
-- MAGIC ];
-- MAGIC
-- MAGIC var bcCurrent = null, bcLight = false;
-- MAGIC
-- MAGIC function bcToggle() {
-- MAGIC   bcLight = !bcLight;
-- MAGIC   document.getElementById('bc-code-wrap').classList.toggle('light', bcLight);
-- MAGIC   document.getElementById('bc-theme-btn').textContent = bcLight ? 'Dark Mode' : 'Light Mode';
-- MAGIC }
-- MAGIC
-- MAGIC function bcSelect(id) {
-- MAGIC   var code = document.getElementById('bc-code');
-- MAGIC   var card = document.getElementById('bc-explain-card');
-- MAGIC   document.querySelectorAll('.f1-card').forEach(function(b) {
-- MAGIC     b.classList.toggle('active', parseInt(b.dataset.id) === id);
-- MAGIC   });
-- MAGIC   if (bcCurrent === id) {
-- MAGIC     code.classList.remove('has-highlight');
-- MAGIC     card.classList.remove('visible');
-- MAGIC     document.querySelectorAll('.f1-card').forEach(function(b) { b.classList.remove('active'); });
-- MAGIC     bcCurrent = null;
-- MAGIC     return;
-- MAGIC   }
-- MAGIC   bcCurrent = id;
-- MAGIC   var c = BC_DATA[id];
-- MAGIC   code.classList.add('has-highlight');
-- MAGIC   code.querySelectorAll('.line').forEach(function(ln) {
-- MAGIC     var g = ln.dataset.g;
-- MAGIC     ln.classList.toggle('hl', g === String(id) || g === 'all');
-- MAGIC   });
-- MAGIC   var bulletsHtml = c.bullets.map(function(b) { return '<li>' + b + '</li>'; }).join('');
-- MAGIC   card.style.borderTopColor = c.color;
-- MAGIC   card.innerHTML =
-- MAGIC     '<span class="f1-badge" style="background:' + c.badgeBg + ';color:' + c.color + ';">' + c.badge + '</span>' +
-- MAGIC     '<div style="font-size:17pt;font-weight:700;margin-bottom:10px;color:#0b2026;font-family:monospace;">' + c.title + '</div>' +
-- MAGIC     '<div style="margin-bottom:10px;">' + c.text + '</div>' +
-- MAGIC     '<ul>' + bulletsHtml + '</ul>';
-- MAGIC   card.classList.add('visible');
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D4. STEP 3: Processing CDC Data with **`AUTO CDC INTO`**
-- MAGIC Spark Declarative Pipelines introduces a new syntactic structure for simplifying CDC feed processing: `AUTO CDC INTO` (formerly `APPLY CHANGES INTO`).
-- MAGIC
-- MAGIC The code in **STEP 3** of the **customers_pipeline.sql** file uses `AUTO CDC INTO` to:
-- MAGIC
-- MAGIC - **CREATES** 
-- MAGIC   - **sdp_2_silver.scd_type_1_customers_silver_demo6** streaming table if it doesn't exist,
-- MAGIC
-- MAGIC - **UPDATES** 
-- MAGIC   - **sdp_2_silver.scd_type_1_customers_silver_demo6** streaming table with updates, inserts and deletes using records from the **sdp_1_bronze.customers_bronze_clean_demo6** streaming table.
-- MAGIC
-- MAGIC #### Additional Notes
-- MAGIC **`AUTO CDC INTO`** has the following guarantees and requirements:
-- MAGIC - Performs incremental/streaming ingestion of CDC data
-- MAGIC - Provides simple syntax to specify one or many fields as the primary key for a table
-- MAGIC - Default assumption is that rows will contain inserts and updates
-- MAGIC - Can optionally apply deletes
-- MAGIC - Automatically orders late-arriving records using user-provided sequencing key (order to process rows)
-- MAGIC - Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
-- MAGIC - The default to applying changes is SCD Type 1. You can also use SCD Type 2 if you would like. We will focus on SCD Type 1.
-- MAGIC
-- MAGIC
-- MAGIC #### Documentation
-- MAGIC AUTO CDC INTO (Lakeflow Spark Declarative Pipelines):
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-sql-ref-apply-changes-into) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt-ref/dlt-sql-ref-apply-changes-into)
-- MAGIC
-- MAGIC The AUTO CDC APIs - Simplify change data capture with Lakeflow Spark Declarative Pipelines:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/cdc) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/cdc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D5. STEP 4: Explore the Customers Pipeline Graph
-- MAGIC After running the pipeline and reviewing the code cells, take time to explore the pipeline results for the **customers** flow following the steps below.
-- MAGIC
-- MAGIC **Run with 1 JSON File**
-- MAGIC
-- MAGIC ![demo6_cdc_run01.png](./Includes/images/change-data-capture/demo6_cdc_run_1.png)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #1976d2;
-- MAGIC   background: #e3f2fd;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
-- MAGIC     View the Results
-- MAGIC   </strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Notice the following:
-- MAGIC 1. In the **customers** flow in the pipeline graph, notice that **939** rows were streamed into the three streaming tables.
-- MAGIC     - This is because all records are new and valid entries, they were ingested throughout the flow.
-- MAGIC
-- MAGIC 2. In the table window below, find the **scd_type_1_customers_silver_demo6** table and select **Table metrics**. 
-- MAGIC
-- MAGIC     Note the following:
-- MAGIC
-- MAGIC     - The **Upserted** column indicates that all **939** rows were upserted into the table, as all rows are new.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D6. STEP 5: Explore the Customers Pipeline Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the query below to view the **scd_type_1_customers_silver_demo6** streaming table (the table with SCD Type 1 updates, inserts and deletes).
-- MAGIC
-- MAGIC     Notice the following after the first run ingestion the **00.json** file:
-- MAGIC
-- MAGIC    - The streaming table contains all **939 rows** from the **00.json** file, since they are all new customers being added to the target table.
-- MAGIC
-- MAGIC    - Each record was inserted into the empty streaming table.

-- COMMAND ----------

-- DBTITLE 1,Explore the CDC SCD1 Streaming Table
SELECT *
FROM sdp_2_silver.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **scd_type_1_customers_silver_demo6** streaming table for the following **customer_id** values (*23225*, *23617*).
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC       - **customer_id** = *23225*
-- MAGIC          - **Address**: `76814 Jacqueline Mountains Suite 815`
-- MAGIC          - **State**: `TX`
-- MAGIC       - **customer_id** = *23617*
-- MAGIC          - This customer exists in the first execution (in file **00.json**)

-- COMMAND ----------

-- DBTITLE 1,View a customer in the CDC ST
SELECT *
FROM sdp_2_silver.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Land New Data to Your Data Source Volume
-- MAGIC Complete the following after executing and reviewing the **customers** pipeline flow that consisted of ingesting one file (**00.json**) from cloud storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to land a new JSON file to each volume (**customers**, **status** and **orders**) to simulate new files being added to your cloud storage locations.

-- COMMAND ----------

-- DBTITLE 1,Land a new file in your volume
-- MAGIC %python
-- MAGIC
-- MAGIC ## Find data in workspace data folder
-- MAGIC data_path = find_folder('Includes/data')
-- MAGIC
-- MAGIC ## Land JSON files to your orders volume
-- MAGIC copy_workspace_files_to_volume(
-- MAGIC     src_workspace_folder=f'{data_path}/orders',
-- MAGIC     target_volume_path=f'{source_volume_path}/orders',
-- MAGIC     n=2
-- MAGIC )
-- MAGIC
-- MAGIC ## Land JSON files to your status volume
-- MAGIC copy_workspace_files_to_volume(
-- MAGIC     src_workspace_folder=f'{data_path}/status',
-- MAGIC     target_volume_path=f'{source_volume_path}/status',
-- MAGIC     n=2
-- MAGIC )
-- MAGIC
-- MAGIC ## Land JSON files to your customers volume
-- MAGIC copy_workspace_files_to_volume(
-- MAGIC     src_workspace_folder=f'{data_path}/customers',
-- MAGIC     target_volume_path=f'{source_volume_path}/customers',
-- MAGIC     n=2
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to programmatically view the files in your `/Volumes/labuser/sdp_1_bronze/source/customers` volume.
-- MAGIC
-- MAGIC     Confirm your volume now contains the original **00.json** file and the new **01.json** file.

-- COMMAND ----------

-- DBTITLE 1,View files in your customers volume
-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{source_volume_path}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell to explore the raw data in the new **01.json** file prior to ingesting it in your pipeline.
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC
-- MAGIC    - This file contains **23** rows.
-- MAGIC
-- MAGIC    - The **operation** column specifies **UPDATE**, **DELETE**, and **NEW** operations for customers.
-- MAGIC       - **In the new 01.json file there are**:
-- MAGIC          - 12 customers with **UPDATE** values
-- MAGIC          - 1 customer with a **DELETE** value
-- MAGIC          - 10 new customers with a **NEW** value

-- COMMAND ----------

-- DBTITLE 1,Explore the new 01.json file
SELECT *
FROM read_files(
  source_volume_path || '/customers/01.json',
  format => "JSON"
)
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the cell to view **customer_id** values *23225* and *23617* in the **01.json** file.
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23225* and note the following:
-- MAGIC
-- MAGIC       - The original address for **Sandy Adams** (from the streaming table, file **00.json**) was: `76814 Jacqueline Mountains Suite 815`, `TX`
-- MAGIC       - The updated address for **Sandy Adams** (from the file below) is: `512 John Stravenue Suite 239`, `TN`
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23617* and note the following:
-- MAGIC       - The **operation** for this customer is **DELETE**.
-- MAGIC       - When the **operation** column is delete, all other column values are `null`.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM read_files(
  source_volume_path || '/customers/01.json',
  format => "JSON"
)
WHERE customer_id IN (23225, 23617)
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Run the SDP with the New File
-- MAGIC
-- MAGIC ##### Go back to your pipeline and click `Run pipeline` button to ingest the new JSON file (**01.json**) incrementally and perform CDC SCD Type 1 on the `scd_type_1_customers_silver_demo6` table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Explore the Customers Pipeline
-- MAGIC
-- MAGIC After you have explored and landed 1 new JSON file into each of your cloud data sources, complete the following to explore the **customers** flow in the **Pipeline graph**:
-- MAGIC
-- MAGIC a. 23 rows were read into the:
-- MAGIC
-- MAGIC   - **customers_bronze_raw_demo6** streaming table
-- MAGIC   - **customers_bronze_clean_demo6** streaming table (all data quality checks passed)
-- MAGIC   - The pipeline only ingested and processed the NEW **01.json** file
-- MAGIC
-- MAGIC b. In the **scd_type_1_customers_silver_demo6** streaming table details (The CDC SCD Type 1 table) it contains:
-- MAGIC   - **Upserted = 22**:
-- MAGIC     - 12 customers with UPDATE values (previous customer were simply updated with the new values)
-- MAGIC     - 10 new customers with a NEW value (new customers were inserted into the table)
-- MAGIC   - **Deleted records = 1**:
-- MAGIC     - 1 customer was marked as DELETE and deleted from the table
-- MAGIC
-- MAGIC ![Run 2](./Includes/images/change-data-capture/demo6_cdc_run_2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Explore the CDC SCD Type 1 on the `scd_type_1_customers_silver_demo6` Streaming Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the data in the **scd_type_1_customers_silver_demo6** streaming table with SCD Type 1 and observe the following:
-- MAGIC
-- MAGIC    a. The table contains **948 rows**:
-- MAGIC       - **initial 939 customers**
-- MAGIC       - \+ **10** new customers
-- MAGIC       - \- **1** deleted customer
-- MAGIC       - **NOTES:**
-- MAGIC          - The **12** updates to original customers were made in place and updated the original record (SCD Type 1 does not keep historical records).
-- MAGIC          - The **1** record marked for deletion was deleted from the table.

-- COMMAND ----------

-- DBTITLE 1,View the updated CDC ST
SELECT customer_id, address, name
FROM sdp_2_silver.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **sdp_2_silver.scd_type_1_customers_silver_demo6** table for the following **customer_id** values: *23225* and *23617*. These were the values we reviewed earlier.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC
-- MAGIC     - **customer_id** *23225* has been updated to the new address. The historical address was not retained because we used SCD Type 1.
-- MAGIC     - **customer_id** *23617* has been deleted from the table. It no longer exists because we used SCD Type 1.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the updated customer 23225
SELECT *
FROM sdp_2_silver.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - What is change data capture (CDC)?:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/what-is-change-data-capture) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/what-is-change-data-capture) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/what-is-change-data-capture)
-- MAGIC
-- MAGIC - AUTO CDC INTO (Lakeflow Spark Declarative Pipelines) documentation:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-sql-ref-apply-changes-into) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt-ref/dlt-sql-ref-apply-changes-into)
-- MAGIC
-- MAGIC - The AUTO CDC APIs - Simplify change data capture with Lakeflow Spark Declarative Pipelines documentation:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/cdc) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/cdc)
-- MAGIC
-- MAGIC - [How to implement Slowly Changing Dimensions when you have duplicates - Part 1: What to look out for?](https://community.databricks.com/t5/technical-blog/how-to-implement-slowly-changing-dimensions-when-you-have/ba-p/40568)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
-- MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
-- MAGIC <script>
-- MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
-- MAGIC </script>