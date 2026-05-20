-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![Databricks Academy](./Includes/images/common/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8 Lab - AUTO CDC INTO with SCD Type 1
-- MAGIC
-- MAGIC ##### NOTE: The AUTO CDC APIs replace the APPLY CHANGES APIs, and have the same syntax. The APPLY CHANGES APIs are still available, but Databricks recommends using the AUTO CDC APIs in their place.
-- MAGIC
-- MAGIC ### Estimated Duration: ~15-20 minutes
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Use `AUTO CDC INTO` to perform Change Data Capture (CDC) using SCD Type 1.

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
-- MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.1em;">This is an optional lab that can be completed after class if you're interested in practicing CDC</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC In this lab, you will use Change Data Capture (CDC) to detect changes and apply them using SCD Type 1 logic (overwrite, no historical records).
-- MAGIC
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
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
-- MAGIC Run the following cell to configure your working environment for this lab.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-Lab-cdc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. SCENARIO
-- MAGIC
-- MAGIC Your data engineering team wants to build a Lakeflow Spark Declarative Pipeline to maintain a record of current employees without keeping historical data (SCD Type 1).
-- MAGIC
-- MAGIC The project has been started, but the final step of **updating the silver table with current employee records** has not yet been completed.
-- MAGIC
-- MAGIC There are already two files in a cloud storage location that contain information about employees and employee updates.
-- MAGIC
-- MAGIC ### REQUIREMENTS:
-- MAGIC It's your job to complete the Spark Declarative Pipeline by adding the `AUTO CDC INTO` statement to perform SCD Type 1.
-- MAGIC
-- MAGIC Follow the steps below to complete your task.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
-- MAGIC
-- MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
-- MAGIC
-- MAGIC   <img src="./Includes/images/common/genie-code.png" style="height: 44px; margin-bottom: 10px;">
-- MAGIC
-- MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 12px;">
-- MAGIC     Need Help? Use Genie Code
-- MAGIC   </div>
-- MAGIC
-- MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.6; margin-bottom: 16px;">
-- MAGIC     Genie is an AI-powered assistant that can help you as you work through this lab. 
-- MAGIC     Use it if you get stuck or want a little extra guidance.
-- MAGIC   </div>
-- MAGIC
-- MAGIC   <a href="https://docs.databricks.com/aws/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">AWS</a> |
-- MAGIC   <a href="https://learn.microsoft.com/en-us/azure/databricks/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">Azure</a> |
-- MAGIC   <a href="https://docs.databricks.com/gcp/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">GCP</a>
-- MAGIC
-- MAGIC </div>
-- MAGIC
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore the Raw Data Source Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/labuser/sdp_lab_1_bronze/lab_files` volume.
-- MAGIC
-- MAGIC     Confirm that you see **employees_1.csv** and **employees_2.csv**.
-- MAGIC
-- MAGIC **NOTE:** You can also manually navigate to your **labuser.sdp_lab_1_bronze.lab_files** volume and view the files in the volume.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View files in the lab_files volume
-- MAGIC %python
-- MAGIC spark.sql(f'LIST "/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the 2 CSV files in that volume.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC
-- MAGIC     - The files contain a list of employees.
-- MAGIC
-- MAGIC     - The **employees_1.csv** contains the initial employees.
-- MAGIC
-- MAGIC     - The **employees_2.csv** contains an update, a delete, and two new employees.
-- MAGIC
-- MAGIC     - The **Operation** column provides information about the action for each record (new employee, update employee information, or delete employee).
-- MAGIC
-- MAGIC     - The **ProcessDate** column indicates when the records were processed (acts as a sequence column).
-- MAGIC
-- MAGIC     - In total, there are 10 rows.
-- MAGIC
-- MAGIC         - There are two duplicate **EmployeeID** values:
-- MAGIC           - **EmployeeID 1** – Sophia was an employee, then should be **deleted**.
-- MAGIC           - **EmployeeID 3** – Liam received a bonus, and his **Salary** needs to be **updated**.
-- MAGIC
-- MAGIC         - **Employee 6 & 7** - New employees from the **employees_2.csv** file.

-- COMMAND ----------

-- DBTITLE 1,View the volume CSV files
SELECT
  _metadata.file_name as source_file,
  *
FROM read_files(
  '/Volumes/' || my_catalog || '/sdp_lab_1_bronze/lab_files',
  format => 'CSV'
)
ORDER BY source_file, EmployeeID, ProcessDate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Looking at the output from above, our final table after applying SCD Type 1 on the two files should:
-- MAGIC
-- MAGIC    - Contain 6 rows of data:
-- MAGIC       - remove the **EmployeeID** with a `null` value (removed with a data quality expectation)
-- MAGIC       - delete **EmployeeID** 1 (employee who left)
-- MAGIC
-- MAGIC    - **EmployeeID 3** should have a current salary of 100,000 and only one row of data.
-- MAGIC
-- MAGIC    - **EmployeeID 6 & 7** are new employees from **employees_2.csv** file.
-- MAGIC
-- MAGIC    - No historical data should be tracked.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC
-- MAGIC **FINAL TABLE OUTPUT**
-- MAGIC | EmployeeID | FirstName | Country | Department | Salary | HireDate   | ProcessDate |
-- MAGIC |------------|-----------|---------|------------|--------|------------|-------------|
-- MAGIC | 2          | Nikos     | GR      | IT         | 55000  | 2025-04-10 | 2025-06-05  |
-- MAGIC | 3          | Liam      | US      | Sales      | **100000** | 2025-05-03 | **2025-06-22**  |
-- MAGIC | 4          | Elena     | GR      | IT         | 53000  | 2025-06-04 | 2025-06-05  |
-- MAGIC | 5          | James     | US      | IT         | 60000  | 2025-06-05 | 2025-06-05  |
-- MAGIC | 6          | Emily     | US      | Enablement | 80000  | 2025-06-09 | **2025-06-22**  |
-- MAGIC | 7          | Yannis    | GR      | HR         | 70000  | 2025-06-20 | **2025-06-22**  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. TO DO: Complete the Pipeline with SCD Type 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Create the Spark Declarative Pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to create your starter Spark Declarative Pipeline for this lab. The pipeline will set the following for you:
-- MAGIC     - Your default catalog: **labuser**
-- MAGIC     - Your configuration parameter: `source` = `/Volumes/labuser/sdp_lab_1_bronze/lab_files`
-- MAGIC
-- MAGIC     **NOTE:** If the pipeline already exists, an error will be returned. In that case, you'll need to delete the existing pipeline and rerun this cell.
-- MAGIC
-- MAGIC     To delete the pipeline:
-- MAGIC
-- MAGIC     a. Select **Jobs and Pipelines** from the far-left navigation bar.
-- MAGIC
-- MAGIC     b. Find the pipeline you want to delete.
-- MAGIC
-- MAGIC     c. Click the three-dot menu ![ellipsis icon](./Includes/images/common/ellipsis_icon.png).
-- MAGIC
-- MAGIC     d. Select **Delete**.
-- MAGIC
-- MAGIC **NOTE:**  The `create_declarative_pipeline` function is a custom function built for this course to create the sample pipeline using the Databricks REST API. This avoids manually creating the pipeline and referencing the pipeline assets.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_declarative_pipeline(
-- MAGIC     pipeline_name=f'7 - CDC Lab Starter Project - {my_catalog}',
-- MAGIC     root_path_folder_name='7 - CDC Lab Starter Project',
-- MAGIC     catalog_name=my_catalog,
-- MAGIC     schema_name='default',
-- MAGIC     source_folder_names=['cdc_type_1_pipeline'],
-- MAGIC     configuration={
-- MAGIC         'source': f'/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files'
-- MAGIC     }
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to open the starter Spark Declarative Pipeline project for this lab:
-- MAGIC
-- MAGIC    a. In the main navigation bar, right-click on **Jobs & Pipelines** and select **Open Link in New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select your **7 - CDC Lab Starter Project - labuser** pipeline.
-- MAGIC       - **REQUIRED:** At the top near your pipeline name, turn on **New pipeline monitoring**.
-- MAGIC
-- MAGIC    c. In the **Pipeline details** pane on the far right, select **Open in Editor** (link to the right of **Source code**) to open the pipeline in the **Lakeflow Pipeline Editor**.
-- MAGIC
-- MAGIC    d. In the new tab you should see the folder: **cdc_type_1_pipeline**.
-- MAGIC
-- MAGIC    e. Open the **cdc_type_1_pipeline** folder and select the **cdc_employees.sql** file.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Complete the `cdc_employees.sql` File with `AUTO CDC INTO`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Review the code in the `cdc_employees.sql` file and complete the `AUTO CDC INTO` statement to perform SCD Type 1.
-- MAGIC     - For simplicity in training, all code for the pipeline is in one file **cdc_employees.sql**.
-- MAGIC
-- MAGIC     - Walk through the **cdc_employees.sql** file and read the comments.
-- MAGIC
-- MAGIC     - The **bronze** and **silver** table code is completed for you. You just need to complete the `AUTO CDC INTO` statement.
-- MAGIC
-- MAGIC     - If you need the solution for `AUTO CDC INTO`, expand the cell below.
-- MAGIC
-- MAGIC   2. Complete the `cdc_employees.sql` file and run the pipeline.
-- MAGIC
-- MAGIC AUTO CDC INTO (Lakeflow Spark Declarative Pipelines):
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-sql-ref-apply-changes-into) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt-ref/dlt-sql-ref-apply-changes-into)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ##### ANSWER
-- MAGIC
-- MAGIC <details>
-- MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
-- MAGIC
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
-- MAGIC -- Create the empty streaming table
-- MAGIC CREATE OR REFRESH STREAMING TABLE sdp_lab_2_silver.current_employees_silver_demo7;
-- MAGIC
-- MAGIC -- Perform CDC SCD Type 1
-- MAGIC CREATE FLOW scd_type_1_flow AS
-- MAGIC AUTO CDC INTO sdp_lab_2_silver.current_employees_silver_demo7  -- Target table to update with SCD Type 1 (or 2)
-- MAGIC FROM STREAM sdp_lab_1_bronze.employees_bronze_clean_demo7      -- Source streaming table
-- MAGIC KEYS (EmployeeID)
-- MAGIC APPLY AS DELETE WHEN Operation = 'delete'
-- MAGIC SEQUENCE BY ProcessDate
-- MAGIC COLUMNS * EXCEPT (Operation)
-- MAGIC STORED AS SCD TYPE 1;
-- MAGIC <!-------------------END SOLUTION CODE------------------->
-- MAGIC </code></pre>
-- MAGIC
-- MAGIC
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC
-- MAGIC   const text = el.innerText;
-- MAGIC
-- MAGIC   // Preferred modern API
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     console.error("Fallback copy failed:", err);
-- MAGIC     alert("Could not copy to clipboard. Please copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>
-- MAGIC </details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Explore Your CDC SCD Type 1 Streaming Table
-- MAGIC
-- MAGIC After you have completed the `AUTO CDC INTO` statement in the **cdc_employees.sql** file, compare your results to the solution image below.
-- MAGIC
-- MAGIC **FINAL PIPELINE RUN**
-- MAGIC
-- MAGIC ![Lab 7 Pipeline Run](./Includes/images/lab-2/lab_7_pipelinerun.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to view the data in your **sdp_lab_2_silver.current_employees_silver_demo7** streaming table that applied SCD Type 1, and compare it to the solution below.
-- MAGIC
-- MAGIC     Notice that with SCD Type 1, no historical data is kept.
-- MAGIC
-- MAGIC **FINAL TABLE SOLUTION**
-- MAGIC | EmployeeID | FirstName | Country | Department | Salary | HireDate   | ProcessDate |
-- MAGIC |------------|-----------|---------|------------|--------|------------|-------------|
-- MAGIC | 2          | Nikos     | GR      | IT         | 55000  | 2025-04-10 | 2025-06-05  |
-- MAGIC | 3          | Liam      | US      | Sales      | **100000** | 2025-05-03 | **2025-06-22**  |
-- MAGIC | 4          | Elena     | GR      | IT         | 53000  | 2025-06-04 | 2025-06-05  |
-- MAGIC | 5          | James     | US      | IT         | 60000  | 2025-06-05 | 2025-06-05  |
-- MAGIC | 6          | Emily     | US      | Enablement | 80000  | 2025-06-09 | **2025-06-22**  |
-- MAGIC | 7          | Yannis    | GR      | HR         | 70000  | 2025-06-20 | **2025-06-22**  |

-- COMMAND ----------

SELECT *
FROM sdp_lab_2_silver.current_employees_silver_demo7
ORDER BY EmployeeID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. CHALLENGE SCENARIO
-- MAGIC ### Duration: ~10 minutes
-- MAGIC
-- MAGIC **NOTE:** *If you finish early in a live class, feel free to complete the challenge below. The challenge is optional and most likely won't be completed during the live class. Only continue if your Spark Declarative Pipeline was set up correctly in the previous section by comparing your pipeline to the solution image.*
-- MAGIC
-- MAGIC **SCENARIO:** In the challenge, you will land a new CSV file in your **lab_files** cloud storage volume and rerun the pipeline to watch the Spark Declarative Pipeline perform CDC SCD Type 1 on the new data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to land another file in your **lab_files** cloud storage location and confirm that 3 CSV files exist.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Find data in workspace data folder
-- MAGIC data_path = find_folder('Includes/data')
-- MAGIC
-- MAGIC ## Land JSON files to your orders volume
-- MAGIC copy_workspace_files_to_volume(
-- MAGIC     src_workspace_folder=f'{data_path}/lab_files',
-- MAGIC     target_volume_path=f'/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files',
-- MAGIC     n=3
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC spark.sql(f"LIST '/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files'").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **employees_3.csv** file. Notice the following:
-- MAGIC
-- MAGIC    - **EmployeeID** values **2** and **6** need to be removed.
-- MAGIC
-- MAGIC    - **EmployeeID 8** is a new employee in our company.
-- MAGIC

-- COMMAND ----------

SELECT
  _metadata.file_name as source_file,
  *
FROM read_files(
  '/Volumes/' || my_catalog || '/sdp_lab_1_bronze/lab_files/employees_3.csv',
  format => 'CSV'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Go back to your pipeline and select **Run pipeline**. Examine the pipeline run. Confirm it shows the following:
-- MAGIC
-- MAGIC ![Lab 7 Challenge Run](./Includes/images/lab-2/lab_7_challengesolution.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the cell below to query the table **sdp_lab_2_silver.current_employees_silver_demo7** and view the results. Notice that:
-- MAGIC
-- MAGIC    - The two employees (**EmployeeID** 2 and 6) were deleted.
-- MAGIC
-- MAGIC    - **EmployeeID 8** was added.
-- MAGIC
-- MAGIC    - No historical data is kept with SCD Type 1.
-- MAGIC
-- MAGIC     **NOTE:** If you ran the solution pipeline, the streaming table is named **sdp_lab_2_silver.current_employees_silver_demo7_solution**.
-- MAGIC
-- MAGIC
-- MAGIC     **FINAL TABLE**
-- MAGIC | EmployeeID | FirstName  | Country | Department | Salary  | HireDate   | ProcessDate |
-- MAGIC |------------|------------|---------|------------|---------|------------|-------------|
-- MAGIC | 3          | Liam       | US      | Sales      | 100000  | 2025-05-03 | 2025-06-22  |
-- MAGIC | 4          | Elena      | GR      | IT         | 53000   | 2025-06-04 | 2025-06-05  |
-- MAGIC | 5          | James      | US      | IT         | 60000   | 2025-06-05 | 2025-06-05  |
-- MAGIC | 7          | Yannis     | GR      | HR         | 70000   | 2025-06-20 | 2025-06-22  |
-- MAGIC | 8          | Panagiotis | GR      | Enablement | 90000   | 2025-07-01 | 2025-07-22  |

-- COMMAND ----------

SELECT *
FROM sdp_lab_2_silver.current_employees_silver_demo7
ORDER BY EmployeeID;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
-- MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
-- MAGIC <script>
-- MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
-- MAGIC </script>