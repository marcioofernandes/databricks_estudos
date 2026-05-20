-- Databricks notebook source
-- MAGIC %md
-- MAGIC Consultar Arquivos
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/query/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Querys em Arquivos**
-- MAGIC
-- MAGIC Digamos que você tem arquivos salvos em um bucket, não são tabelas, são arquivos, porém antes de transformar eles em tabela, você precisa realizar consultas nesses arquivos, é possível fazer via python, pyspark, mas durante este notebook iremos focar em como fazer o ETL com SQL 
-- MAGIC
-- MAGIC A estrutura básica da query é: 
-- MAGIC
-- MAGIC SELECT * FROM formato_arquivo.`/path/to/file`
-- MAGIC
-- MAGIC - **formato_arquivo**: Exemplo Json,parquet,CSV,TSV,txt
-- MAGIC - **/path/to/file**:   Caminho da pasta ou do arquivo
-- MAGIC
-- MAGIC Se dentro da pasta existir diversos arquivos diferentes você pode definir a consulta com o path final do arquivo, exemplo `file.json` ou agregar todos os arquivos em uma vizualição única passando apenas o path da pasta. 
-- MAGIC
-- MAGIC **nota**: Para fazer isso, todos os arquivos da pasta devem conter o mesmo schema. 
-- MAGIC
-- MAGIC
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

-- MAGIC %md
-- MAGIC A pasta **ls 'dbfs:/databricks-datasets'** é pública e contém diversos datasets para treinos, pode seguir com os comandos abaixo que irá funcionar
-- MAGIC

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
-- MAGIC dbutils.fs.ls("/databricks-datasets")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Datasets para treino '/databricks-datasets'
-- MAGIC display(dbutils.fs.ls('/databricks-datasets'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pasta='dbfs:/databricks-datasets/bikeSharing/data-001/'
-- MAGIC display(dbutils.fs.ls(pasta))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O comando de SQL básico para ler os arquivos é sempre o mesmo, basta trocar o formato da extensão após o `from`. 
-- MAGIC
-- MAGIC Porém no caso dos arquivos .csv (e alguns outros) o comando não funciona direito, ao rodar a célula abaixo vai notar que: 
-- MAGIC
-- MAGIC 1. os nomes das colunas ficaram errados, na verdade os nomes das colunas foram atribuidas como primeira linha e gerado outros nomes. 
-- MAGIC 2. Os datatypes ficaram todos como string 

-- COMMAND ----------

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

-- Base para ler arquivos csv, nem sempre precisa passar todas configs 
-- mode "FAILFAST" aborts file parsing with a RuntimeException if malformed lines are encountered
SELECT * FROM read_files(
  'abfss://<bucket>@<storage-account>.dfs.core.windows.net/<path>/<file>.csv',
  format => 'csv',
  header => true,
  mode => 'FAILFAST', 
  schema => 'id string, date date, event_time timestamp')

-- COMMAND ----------

-- Aqui a grande diferença é que ao invés de passarmos o format após o FROM, passamos o "read_file" e todas as configuração dentro do parenteses 

select * from read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv', -- primeiro vem o path do arquivo 
    format => 'csv', -- depois colocamos o formato, podendo ser qualquer outro
    header => true -- aqui estamos falando para o databricks interpretar a primeira linha como cabeçalho
)

-- nota que ele inferiu os data types automaticamente. 

-- COMMAND ----------

select * from read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv',
    format => 'csv', 
    header => true, 
    inferschema => true -- aqui estamos declarando que é para ele inferir o schema, mas eu ACHO que ele ja faz isso por padrão, como vimos na célula acima 
)

-- COMMAND ----------

select * from read_files(
    'dbfs:/databricks-datasets/bikeSharing/data-001/*.csv', 
    format => 'csv', 
    header => true, 
    schema => 'instant string, dteday date, season double' -- Neste caso estamos declarando o schema manualmente
)

-- COMMAND ----------

-- read_files utilizando o format json
select * from read_files ('dbfs:/databricks-datasets/iot/iot_devices.json',
   format => 'json'

)


-- COMMAND ----------

SELECT *
FROM read_files('dbfs:/databricks-datasets/iot/*.json') -- Também é possível ler a pasta inteira dos arquivos 
WHERE lcd = 'red'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Nota importante** 
-- MAGIC
-- MAGIC Aqui não criamos nenhuma tabela, isso pode confundir pois a visualização parece uma, e estamos usando SQL, mas na verdade estamos apenas lendo os arquivos, e fazendo consultas em cima deles 
