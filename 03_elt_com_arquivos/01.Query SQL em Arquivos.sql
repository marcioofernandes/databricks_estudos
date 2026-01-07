-- Databricks notebook source
-- MAGIC %md
-- MAGIC Consultar Arquivos
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/query/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Querys em Arquivos**
-- MAGIC
-- MAGIC Algumas formas de Se criar consulta traves do arquivo 
-- MAGIC
-- MAGIC SELECT * FROM formato_arquivo.`/path/to/file`
-- MAGIC
-- MAGIC - **formato_arquivo**: Exemplo Json,parquet,CSV,TSV,txt
-- MAGIC - **/path/to/file**:   Caminho da pasta ou do arquivo
-- MAGIC
-- MAGIC Se dentro da mesma pasta tiver varios arquivos de formatos diferentes  voce pode determinar para pegar dentro da pasta só de um formato desejado 
-- MAGIC Exemplo: file_*.json
-- MAGIC
-- MAGIC Ou capturar uma pasta que dentro tenha varios arquios desde que sja no mesmo schema
-- MAGIC
-- MAGIC Exemplo:/path/dir
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Exemplo Json
SELECT * FROM json.`/path/file_name.json`

-- Exemplo tipos
SELECT * FROM (JSON, CSV,Parquet,TXT).`/path/to/file`

-- Exemplo dentro de pasta completa
SELECT * FROM json.`/path/` -- todos arquivos da pasta no mesmo formato 

-- Exemplo Json selecionando a extensão dentro de uma pasta
SELECT * FROM json.`/path/*.json`

select * from binaryFile.`/path/to/file`

/* Obs:o caminho do arquivo deve ficar entre backticks/crases (``) Usa Shift + (`) normalmente localizado do lado da tecla P



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Comparação Prática para SQL no Databricks

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW comparison_doc AS
SELECT 
  'Definição do esquema' AS Caracteristica,
  'Automática, embutido no arquivo' AS Parquet_JSON,
  'Necessário definir manualmente ou inferir' AS CSV_TSV
UNION ALL
SELECT 
  'Criação da tabela',
  'Simples, sem opções extras',
  'Requer especificar header, delimiter, etc.'
UNION ALL
SELECT 
  'Desempenho',
  'Melhor devido à otimização colunar',
  'Menor, leitura linha a linha'
UNION ALL
SELECT 
  'Erros de formatação',
  'Pouco provável',
  'Mais comum (delimitadores inconsistentes)'
UNION ALL
SELECT 
  'Exemplo de SQL simples',
  'USING parquet',
  'USING csv OPTIONS (header ''true'')';
SELECT * FROM comparison_doc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Formatos mais comuns e ultilização na prática?
-- MAGIC - **Parquet/JSON:** Ideal para grandes volumes de dados ou onde a performance é essencial. O esquema embutido reduz a necessidade de configuração manual.
-- MAGIC - **CSV/TSV/TXT:** Mais adequado para dados simples ou quando você está lidando com arquivos legados sem suporte a formatos avançados.
-- MAGIC - Se possível, converta arquivos para Parquet no Databricks antes de realizar análises intensivas em SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query em arquivo Json

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/databricks-datasets'

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs
-- MAGIC ls 'dbfs:/databricks-datasets/iot/'

-- COMMAND ----------

-- query no Arquivo json
select * from json.`dbfs:/databricks-datasets/iot/iot_devices.json`

-- COMMAND ----------

-- Selecionando algumas colunas e fazendo filtro 
select 
  cn,
  lcd,
  scale
from json.`dbfs:/databricks-datasets/iot/iot_devices.json`
where lcd = "red"



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query em arquivo Parquet
-- MAGIC

-- COMMAND ----------

select * from parquet.`dbfs:/databricks-datasets/credit-card-fraud/data/part-00000-tid-898991165078798880-9c1caa7b-283d-47c4-9be1-aa61587b3675-0-c000.snappy.parquet`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query em Arquivo CSV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Datasets para treino '/databricks-datasets'
-- MAGIC display(dbutils.fs.ls('/databricks-datasets'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pasta='dbfs:/databricks-datasets/bikeSharing/data-001/'
-- MAGIC display(dbutils.fs.ls(pasta))

-- COMMAND ----------

-- ler simples 
select * from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/day.csv`

-- COMMAND ----------

select count (*) from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/day.csv`

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/databricks-datasets/bikeSharing/data-001/'

-- COMMAND ----------

-- lendos todos que forem Csv dentro de uma pasta
select * from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/`


-- COMMAND ----------

select count (*) from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/`

-- COMMAND ----------

-- lendos todos que forem Csv dentro de uma pasta usando a estensao do arquivo como chave de busca
select * from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/*.csv`

-- COMMAND ----------

select count(*) from csv.`dbfs:/databricks-datasets/bikeSharing/data-001/*.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Resolvendo problema de Cabeçalho CSV quando consulta SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Documentação read_files 
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/query/formats/csv

-- COMMAND ----------

-- opção read files read_files + Opções
select * from read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv',
  format => 'csv',
  header => true
)


-- COMMAND ----------

-- opção read files read_files + Opções + inferindo Schema
select * from read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv',
  format => 'csv',
  header => true,
  inferschema => true
)


-- COMMAND ----------

-- opção read files read_files + Opções + inferindo Schema manualmente
select 
  instant,
  dteday,
  season
from 
read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv',
  format => 'csv',
  header => true,
  schema => 'instant string, dteday date, season double'
)

-- COMMAND ----------

-- read_files com Json  'dbfs:/databricks-datasets/iot/iot_devices.json'
select * from read_files ('dbfs:/databricks-datasets/iot/iot_devices.json',
   format => 'json'

)


-- COMMAND ----------

-- read file com opção de escolher arquivos de uma pasta 
SELECT *
FROM read_files('dbfs:/databricks-datasets/iot/*.json')
WHERE lcd = 'red'
