-- Databricks notebook source
-- MAGIC %md
-- MAGIC - Optimize
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/delta-optimize
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/delta/optimize

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas'

-- COMMAND ----------

DESCRIBE HISTORY vendas

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas/_delta_log/'

-- COMMAND ----------

DESCRIBE DETAIL vendas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Benefícios OPTIMIZE :
-- MAGIC - **Compactação de Arquivos**: Agrupa pequenos arquivos em arquivos maiores, reduzindo a sobrecarga de leitura.
-- MAGIC - **Melhora a Performance de Leitura e Escrita**: Arquivos maiores e mais equilibrados melhoram a eficiência das operações de leitura e escrita.
-- MAGIC
-- MAGIC ZORDER BY
-- MAGIC
-- MAGIC - Colocar as informações da coluna no mesmo conjunto de arquivos.

-- COMMAND ----------

--Ver tabela
select * from vendas

-- COMMAND ----------

--Otimizar tabela para leitura
optimize vendas

-- COMMAND ----------

--Otimizar tabela para leitura + Zorder by
OPTIMIZE vendas
ZORDER BY (Id)

-- COMMAND ----------

-- Obs:O uso de ZORDER BY melhora a performance de leitura quando você filtra os dados pelas colunas que foram ordenadas. Isso ocorre porque o Z-Ordering organiza os dados de forma que os valores próximos estejam fisicamente próximos no armazenamento, reduzindo a quantidade de dados lidos durante a consulta.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Otimização preditiva
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/optimizations/predictive-optimization
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/admin/system-tables/predictive-optimization
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/data-governance/unity-catalog/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Propiedades de Tabelas
-- MAGIC > https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-tblproperties
-- MAGIC

-- COMMAND ----------

SHOW TBLPROPERTIES vendas;

-- COMMAND ----------

ALTER TABLE vendas SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **delta.autoOptimize.optimizeWrite**
-- MAGIC
-- MAGIC > Descrição: Ativa a escrita otimizada automaticamente.
-- MAGIC
-- MAGIC > Função: Compacta pequenos arquivos em arquivos maiores durante a escrita, melhorando a eficiência de leitura e escrita.
-- MAGIC
-- MAGIC - **delta.autoOptimize.autoCompact**
-- MAGIC
-- MAGIC > Descrição: Ativa a compactação automática.
-- MAGIC
-- MAGIC > Função: Realiza a compactação automática de arquivos pequenos em segundo plano após a escrita, mantendo a tabela otimizada sem intervenção manual.

-- COMMAND ----------

-- aplicando Otimização preditiva para todas as tabelas (Não faça em ambiente de produção)
SET spark.databricks.delta.optimizeWrite.enabled = false;
SET spark.databricks.delta.autoCompact.enabled = false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - vacuum
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/sql/language-manual/delta-vacuum
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas'

-- COMMAND ----------

vacuum vendas

-- COMMAND ----------

SHOW TBLPROPERTIES vendas;

-- COMMAND ----------

vacuum vendas RETAIN 0 HOURS

-- COMMAND ----------

-- desativar padrao de 7 dias para Vacuum (obs: nao fazer em ambiente de produção)
ALTER TABLE vendas SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 0 hours',
  'delta.deletedFileRetentionDuration' = 'interval 0 hours'
);

-- COMMAND ----------

select * from vendas

-- COMMAND ----------

--Outro comando para fazer a mesma ação - Desativar o padrao de 7 dias para Vacuum (Global)
set spark.databricks.delta.retentionDurationCheck.enabled=false

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/'

-- COMMAND ----------

-- deltar tabelas 
drop table vendas;

-- COMMAND ----------

drop table vendas_version_2;
