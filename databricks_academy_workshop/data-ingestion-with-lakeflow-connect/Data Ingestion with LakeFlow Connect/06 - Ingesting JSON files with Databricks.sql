-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6 - Ingestão de Arquivos JSON com Databricks
-- MAGIC
-- MAGIC Nesta demonstração, exploraremos como ingerir arquivos JSON e realizar transformações fundamentais específicas de JSON durante a ingestão, incluindo a decodificação de campos codificados e o achatamento de strings JSON aninhadas. Trabalharemos com dados simulados de eventos do Kafka, provenientes do Databricks Marketplace.
-- MAGIC
-- MAGIC ### Objetivos de Aprendizagem
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Ingerir dados JSON brutos no Unity Catalog usando CTAS e `read_files()`.
-- MAGIC - Aplicar múltiplas técnicas para achatar colunas de string JSON com e sem conversão para o tipo STRUCT.
-- MAGIC - Entender a diferença entre `explode()` e `explode_outer()`.
-- MAGIC - Introduzir as capacidades e casos de uso do tipo de dado VARIANT (prévia pública a partir do Q2-2025)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default and you have a Shared SQL warehouse.
-- MAGIC
-- MAGIC <!-- ![Select Cluster](./Includes/images/selecting_cluster_info.png) -->
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC    - Click **More** in the drop-down.
-- MAGIC
-- MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 3. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course in the lab environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **dbacademy** and your default schema is your unique **labuser** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Overview of CTAS with `read_files()` for Ingestion of JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Inspect JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the next cell to verify that there are 11 JSON files located at `/Volumes/dbacademy_ecommerce/v01/raw/events-kafka`.

-- COMMAND ----------

-- DBTITLE 1,List files in volume
LIST '/Volumes/dbacademy_ecommerce/v01/raw/events-kafka'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to view the raw JSON data in the output. Note the following:
-- MAGIC
-- MAGIC    - Each row contains JSON with 6 key/value pairs.
-- MAGIC
-- MAGIC    - The **key** and **value** fields are encoded in base64. Base64 is an encoding scheme that converts binary data into a readable ASCII string.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **Example Output Formatted**
-- MAGIC ```
-- MAGIC {
-- MAGIC     "key": "VUEwMDAwMDAxMDczOTgwNTQ=",
-- MAGIC     "offset": 219255030,
-- MAGIC     "partition": 0,
-- MAGIC     "timestamp": 1593880885085,
-- MAGIC     "topic": "clickstream",
-- MAGIC     "value": "eyJkZXZpY2UiOiJBbmRyb2lkIiwiZWNvbW1lcmNlIjp7fSwiZXZlbnRfbmFtZSI6Im1haW4iLCJldmVudF90aW1lc3R
-- MAGIC     hbXAiOjE1OTM4ODA4ODUwMzYxMjksImdlbyI6eyJjaXR5IjoiTmV3IFlvcmsiLCJzdGF0ZSI6Ik5ZIn0sIml0ZW1zIjp
-- MAGIC     bXSwidHJhZmZpY19zb3VyY2UiOiJnb29nbGUiLCJ1c2VyX2ZpcnN0X3RvdWNoX3RpbWVzdGFtcCI6MTU5Mzg4MDg4NTA
-- MAGIC     zNjEyOSwidXNlcl9pZCI6IlVBMDAwMDAwMTA3Mzk4MDU0In0=",
-- MAGIC }
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,View JSON files as text
SELECT * 
FROM text.`/Volumes/dbacademy_ecommerce/v01/raw/events-kafka`
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell below to see how to use `read_files()` to read the JSON data. Notice the following:
-- MAGIC
-- MAGIC    - The JSON file is cleanly read into a tabular format with 6 columns.
-- MAGIC
-- MAGIC    - The **key** and **value** columns are base64-encoded and returned as STRING data type.
-- MAGIC    
-- MAGIC    - There are no rows in the **_rescued_data** column.

-- COMMAND ----------

-- DBTITLE 1,View JSON file in tabular form
SELECT *
FROM read_files(
  "/Volumes/dbacademy_ecommerce/v01/raw/events-kafka",
  format => "json"
)
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Using CTAS and `read_files()` with JSON
-- MAGIC
-- MAGIC Ingesting JSON files using `read_files()` is as straightforward as reading CSV files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute a célula abaixo para armazenar esses dados brutos na tabela **kafka_events_bronze_raw** e visualizar a tabela. Ao inspecionar os resultados, você notará que:
-- MAGIC
-- MAGIC    - As colunas **key** e **value** são do tipo STRING e contêm dados **codificados em base64**.
-- MAGIC
-- MAGIC    - Isso significa que o conteúdo real foi codificado em formato base64 e armazenado como uma string.
-- MAGIC    
-- MAGIC    - Eles ainda não foram transformados em uma string legível na primeira tabela bronze que criamos.
-- MAGIC
-- MAGIC **NOTA:** A codificação Base64 é comumente usada ao ingerir dados de fontes como filas de mensagens ou plataformas de streaming, onde preservar o formato e evitar corrupção de dados é importante.

-- COMMAND ----------

-- DBTITLE 1,Create the bronze raw from JSON
-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS kafka_events_bronze_raw;


-- Create the Delta table
CREATE TABLE kafka_events_bronze_raw AS
SELECT *
FROM read_files(
  "/Volumes/dbacademy_ecommerce/v01/raw/events-kafka",
  format => "json"
);


-- Display the table
SELECT *
FROM kafka_events_bronze_raw
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Decoding base64 Strings for the Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Vamos analisar a decodificação das colunas **key** e **value** inspecionando seus tipos de dados após aplicar a função `unbase64()`. A função `unbase64` retorna uma string base64 decodificada como binário.
-- MAGIC
-- MAGIC     - **encoded_key**: A coluna **key** original codificada em base64.
-- MAGIC
-- MAGIC     - **decoded_key**: Uma nova coluna criada ao decodificar **key** de uma string base64 para BINARY.
-- MAGIC
-- MAGIC     - **encoded_value**: A coluna **value** original codificada em base64.
-- MAGIC
-- MAGIC     - **decoded_value**: Uma nova coluna criada ao decodificar **value** de uma string base64 para BINARY.
-- MAGIC
-- MAGIC     Execute a célula e observe os resultados. Note que as colunas **decoded_key** e **decoded_value** agora são do tipo BINARY.

-- COMMAND ----------

-- DBTITLE 1,Decode the Base64 string as binary
SELECT
  key AS encoded_key,
  unbase64(key) AS decoded_key,
  value AS encoded_value,
  unbase64(value) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the next cell to convert the BINARY columns to STRING columns using the `CAST` function. Notice the following in the results:
-- MAGIC
-- MAGIC     - The **decoded_key** and **decoded_value** columns are now of type STRING and readable.
-- MAGIC
-- MAGIC     - The **decoded_value** column is a JSON-formatted string.
-- MAGIC

-- COMMAND ----------

select 
    key as chave_criptografada, 
    cast(unbase64(key) as string) as chave_descriptografada, 
    value as valor_cripto, 
    cast( unbase64(value)as string)
from kafka_events_bronze_raw
limit 5; 

-- COMMAND ----------

-- DBTITLE 1,Cast the BINARY as a STRING with CAST
SELECT
  key AS encoded_key,
  cast(unbase64(key) AS STRING) AS decoded_key,
  value AS encoded_value,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Now, let's put it all together to create another bronze-level table named **kafka_events_bronze_decoded**. This table will store the STRING values for the **key** and **value** columns from the original **kafka_events_bronze_raw** table.

-- COMMAND ----------

-- DBTITLE 1,Create the bronze_decoded table
CREATE OR REPLACE TABLE kafka_events_bronze_decoded AS
SELECT
  cast(unbase64(key) AS STRING) AS decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw;


-- View the new table
SELECT *
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Working with JSON Formatted Strings in a Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Achatando Colunas de String JSON
-- MAGIC
-- MAGIC A seguir, vamos explorar como extrair uma coluna de uma coluna que contém uma string formatada em JSON. 
-- MAGIC
-- MAGIC **BENEFÍCIOS**
-- MAGIC - **Simples** - Fácil de implementar e armazenar JSON como texto simples.
-- MAGIC - **Flexível** - Pode conter qualquer estrutura JSON sem restrições de esquema.
-- MAGIC
-- MAGIC **CONSIDERAÇÕES**
-- MAGIC - **Desempenho** - Colunas do tipo STRING são mais lentas ao consultar e processar dados complexos.
-- MAGIC - **Sem Esquema** - A ausência de um esquema definido para colunas STRING pode levar a problemas de integridade dos dados.
-- MAGIC - **Consulta Complexa** - Requer código adicional para analisar e recuperar dados, o que pode ser complexo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C1.1 Consultando strings JSON
-- MAGIC
-- MAGIC Você pode extrair uma coluna de campos que contêm strings JSON usando a sintaxe: `<column-name>:<extraction-path>`, onde `<column-name>` é o nome da coluna string e `<extraction-path>` é o caminho para o campo a ser extraído. Os resultados retornados são strings. Você também pode fazer isso com campos aninhados usando `.` ou `[]`.
-- MAGIC
-- MAGIC Isso utiliza a funcionalidade nativa do Spark SQL para interagir diretamente com dados aninhados armazenados como strings JSON.
-- MAGIC
-- MAGIC [Consultar strings JSON](https://docs.databricks.com/aws/en/semi-structured/json)
-- MAGIC
-- MAGIC
-- MAGIC Exemplo de string JSON extraída de uma linha na coluna **decoded_value**:
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC {
-- MAGIC     "device": "iOS",
-- MAGIC     "ecommerce": {},
-- MAGIC     "event_name": "add_item",
-- MAGIC     "event_previous_timestamp": 1593880300696751,
-- MAGIC     "event_timestamp": 1593880892251310,
-- MAGIC     "geo": {
-- MAGIC       "city": "Westbrook", 
-- MAGIC       "state": "ME"
-- MAGIC       },
-- MAGIC     "items": [
-- MAGIC         {
-- MAGIC             "item_id": "M_STAN_T",
-- MAGIC             "item_name": "Standard Twin Mattress",
-- MAGIC             "item_revenue_in_usd": 595.0,
-- MAGIC             "price_in_usd": 595.0,
-- MAGIC             "quantity": 1,
-- MAGIC         }
-- MAGIC     ],
-- MAGIC     "traffic_source": "google",
-- MAGIC     "user_first_touch_timestamp": 1593880300696751,
-- MAGIC     "user_id": "UA000000107392458",
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. For example, let's extract the following values from the JSON-formatted string:
-- MAGIC     - `decoded_value:device`
-- MAGIC     - `decoded_value:traffic_source`
-- MAGIC     - `decoded_value:geo`
-- MAGIC     - `decoded_value:items`
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that we have successfully extracted the values from the JSON formatted string.
-- MAGIC
-- MAGIC     - **device** is a STRING
-- MAGIC
-- MAGIC     - **traffic_source** is a STRING
-- MAGIC
-- MAGIC     - **geo** is a STRING containing another JSON formatted string
-- MAGIC     
-- MAGIC     - **item** is a STRING contain an array of JSON formatted strings
-- MAGIC

-- COMMAND ----------

select 
    decoded_value, 
    decoded_value:device
from kafka_events_bronze_decoded
limit 5

-- COMMAND ----------

-- DBTITLE 1,Query a JSON string and extract values
SELECT 
  decoded_value,
  decoded_value:device,
  decoded_value:traffic_source,
  decoded_value:geo, ----- Contains another JSON formatted string
  decoded_value:geo:city,
  decoded_value:items      ----- Contains a nested-array of JSON formatted strings
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We can then begin to parse out the necessary JSON formatted string values to create another bronze table to flatten the JSON formatted string column for downstream processing.

-- COMMAND ----------

-- DBTITLE 1,Flatten the JSON formatted string
CREATE OR REPLACE TABLE kafka_events_bronze_string_flattened AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  decoded_value:device,
  decoded_value:traffic_source,
  decoded_value:geo,       ----- Contains another JSON formatted string
  decoded_value:items      ----- Contains a nested-array of JSON formatted strings
FROM kafka_events_bronze_decoded;


-- Display the table
SELECT *
FROM kafka_events_bronze_string_flattened;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Achatando Strings JSON via Conversão para STRUCT
-- MAGIC
-- MAGIC Semelhante à seção anterior, vamos discutir como achatar nossa coluna STRING JSON **decoded_value** usando uma coluna STRUCT.
-- MAGIC
-- MAGIC #### Benefícios e Considerações das Colunas STRUCT
-- MAGIC
-- MAGIC **Benefícios**
-- MAGIC - **Imposição de Esquema** – Colunas STRUCT definem e impõem um esquema, ajudando a manter a integridade dos dados.
-- MAGIC - **Melhor Desempenho** – STRUCTs são geralmente mais eficientes para consultas e processamento do que strings simples.
-- MAGIC
-- MAGIC **Considerações**
-- MAGIC - **Imposição de Esquema** – Como o esquema é imposto, problemas podem surgir se a estrutura do JSON mudar ao longo do tempo.
-- MAGIC - **Flexibilidade Reduzida** – Os dados devem corresponder consistentemente ao esquema definido, deixando menos espaço para variações estruturais.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.1 Convertendo uma STRING JSON em uma Coluna STRUCT
-- MAGIC Para converter uma coluna STRING formatada em JSON em uma coluna STRUCT, você precisará derivar o esquema da string JSON e então analisar cada linha para o tipo STRUCT.
-- MAGIC
-- MAGIC Podemos fazer isso em dois passos.
-- MAGIC   1. Obtenha o tipo STRUCT da string JSON formatada.
-- MAGIC   2. Aplique o STRUCT à coluna de string JSON formatada.
-- MAGIC
-- MAGIC **NOTA:** Já copiamos e colamos os valores corretos para você como parte desta demonstração. A célula subsequente abaixo é um copiar e colar da saída da única linha que aparece ao executar a próxima célula.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Determine the derived schema using the [**`schema_of_json()`**](https://docs.databricks.com/en/sql/language-manual/functions/schema_of_json.html) function, which returns the schema inferred from a JSON-formatted string.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the output displays the structure of the JSON string.

-- COMMAND ----------

-- DBTITLE 1,Determine the schema of the JSON formatted string
SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')
AS schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Copie e cole a saída do `schema_of_json` na função [**`from_json()`**](https://docs.databricks.com/en/sql/language-manual/functions/from_json.html). Essa função analisa uma coluna contendo uma string JSON e a converte para o tipo STRUCT usando o esquema especificado, criando uma nova tabela chamada **kafka_events_bronze_struct**.
-- MAGIC
-- MAGIC     Execute a célula e veja os resultados. Observe que a coluna **value** foi transformada em um STRUCT aninhado que inclui campos escalares, structs aninhados e um array de structs.

-- COMMAND ----------

CREATE OR REPLACE TABLE kafka_events_bronze_struct AS
SELECT 
  * EXCEPT (decoded_value),
  from_json(
      decoded_value,    -- JSON formatted string column
      'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS value
FROM kafka_events_bronze_decoded;


-- View the new table.
SELECT *
FROM kafka_events_bronze_struct
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.2 Extract fields, nested fields, and nested arrays from STRUCT columns
-- MAGIC
-- MAGIC We can query the STRUCT column using `value.device` or `value.ecommerce` in our SELECT statement. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Using this syntax, we can obtain values from the **value** struct column. Run the cell and view the results. Notice the following:
-- MAGIC
-- MAGIC     - We obtained values from the STRUCT column for **device** and **city**
-- MAGIC     
-- MAGIC     - The **items** column contains an ARRAY of STRUCTS. The number of elements in the array varies.

-- COMMAND ----------

-- DBTITLE 1,Obtain values from a STRUCT column
SELECT 
  decoded_key,
  value.device as device,  -- <----- Field
  value.geo.city as city,  -- <----- Nested-field from geo field
  value.items as items,
  array_size(items) AS number_elements_in_array -- <----- Count the number of elements in the array column items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.3 Explode Arrays
-- MAGIC
-- MAGIC Exploding an array transforms each element of an array column into a separate row, effectively flattening the array. There are a few things to keep in mind when using this function. 
-- MAGIC
-- MAGIC 1. It returns a set of rows composed of the elements of the array or the keys and values of the map.
-- MAGIC
-- MAGIC 1. If the array is `NULL` no rows are produced. To return a single row with `NULL`s for the array or map values use the [`explode_outer()`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/explode_outer) function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell to see how the ARRAY of values in the `value.items` explodes the array into one row for each element in the array.

-- COMMAND ----------

-- DBTITLE 1,Explore the array to one row per element
CREATE OR REPLACE TABLE bronze_explode_array AS
SELECT
  decoded_key,
  array_size(value.items) AS number_elements_in_array,
  explode(value.items) AS item_in_array,
  value.items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;


-- Display table
SELECT *
FROM bronze_explode_array;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Working with a VARIANT Column (Public Preview)
-- MAGIC
-- MAGIC #### VARIANT Column Benefits and Considerations:
-- MAGIC
-- MAGIC **BENEFITS**
-- MAGIC - **Open** - Fully open-sourced, no proprietary data lock-in.
-- MAGIC - **Flexible** - No strict schema. You can put any type of semi-structured data into VARIANT.
-- MAGIC - **Performant** - Improved performance over existing methods.
-- MAGIC
-- MAGIC **CONSIDERATIONS**
-- MAGIC - Currently in public preview as of 2025 Q2.
-- MAGIC - [Variant support in Delta Lake](https://docs.databricks.com/aws/en/delta/variant)
-- MAGIC
-- MAGIC **RESOURCES**:
-- MAGIC - [Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)
-- MAGIC - [Say goodbye to messy JSON headaches with VARIANT](https://www.youtube.com/watch?v=fWdxF7nL3YI)
-- MAGIC - [Variant Data Type - Making Semi-Structured Data Fast and Simple](https://www.youtube.com/watch?v=jtjOfggD4YY)
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** Variant data type will not work on Serverless Version 1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the **kafka_events_bronze_decoded** table. Confirm the **decoded_value** column contains a JSON formatted string.

-- COMMAND ----------

SELECT *
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use the [`parse_json`](https://docs.databricks.com/aws/en/sql/language-manual/functions/parse_json) function to returns a VARIANT value from the JSON formatted string.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the **json_variant_value** column is of type VARIANT.

-- COMMAND ----------

-- DBTITLE 1,Create a VARIANT column
CREATE OR REPLACE TABLE kafka_events_bronze_variant AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  parse_json(decoded_value) AS json_variant_value   -- Convert the decoded_value column to a variant data type
FROM kafka_events_bronze_decoded;

-- View the table
SELECT *
FROM kafka_events_bronze_variant
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. You can parse the VARIANT data type column using `:` to create your desired table.
-- MAGIC
-- MAGIC     [VARIANT type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type)

-- COMMAND ----------

SELECT
  json_variant_value,
  json_variant_value:device :: STRING,  -- Obtain the value of device and cast to a string
  json_variant_value:items
FROM kafka_events_bronze_variant
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>