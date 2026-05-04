-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Optimize, Vacuum e Predictive Optimization 
-- MAGIC
-- MAGIC São mecanismos de manutenção que garante o alto desempenho das consultas e o controle de custo de armazenamento 
-- MAGIC
-- MAGIC ### OPTIMIZE 
-- MAGIC
-- MAGIC É utilizado para resolver um problema comum em tabelas streamings chamado de **small file problem**, que é a criação de milhares de arquivos pequenos vindo de ingestões continuas, isso prejudica o tempo de leitura devido a alta sobrecarga de rede e I/O necessário para abrir todos eles. 
-- MAGIC
-- MAGIC **O que o comando faz?** Ele compacta automaticamente a tabela, juntando esses arquivos pequenos em um arquivo grande, normalmente visando o tamanho de 1GB. 
-- MAGIC
-- MAGIC **Z-Ordering**: Junto com o OPTIMIZE é possível aplicar uma técnica chamada de Z ORDER (ZORDER BY) que organiza e agrupa fisicamente os dados com base em colunas especifica (como colunas de datas e categorias). Isso potencializa o mecanismo de "data skipping" permitindo que os dados que não contém valores desejados sejam ignorados na consulta, aumentando muito a performance. 
-- MAGIC
-- MAGIC ### VACUUM (Limpeza fisíca)
-- MAGIC
-- MAGIC Qualquer processo realizado na tabela (ACID) a plataforma mantém o arquivo de metadados fisicamente armazenado na nuvem a fim de preservar a possibilidade de fazer o timetravel dos dados, com o tempo esses arquivos vão acumulando e o custo de armazenamento aumenta. 
-- MAGIC
-- MAGIC O VACUUM  examina a tabela e apaga esses arquivos fisicos que ultrapassam o período de retenção. Por padrão o período de retenção é de 7 dias, ou seja, se você rodar o comando VACUUM ele só apagara os dados que ultrapassarem os 7 dias, mantendo o timetravel para dados de dentro do período. 
-- MAGIC
-- MAGIC Rodar o VACUUM periodicamente além de reduzir drasticamente o custo de armazenamento, também garante conformidade com a lei de exclusão de dados, como a GDPR, removendo arquivos de forma permanente. 
-- MAGIC
-- MAGIC ### PREDICTIVE OPTIMIZATION 
-- MAGIC
-- MAGIC Rodar os comando de manutenção periódicamente pode dar trabalho e é para isso que existe a Otimização Preditiva. É um recurso autônomo do Databricks focado em tabelas geradas pelo Unity Catalog, a plataforma utiliza modelos de custos para analisar padrões de utilização dos dados para determinar a frequencia ideal para rodar comandos de manutenção (VACUUM, OPTIMIZE, ETC)
-- MAGIC
-- MAGIC Elimina a necessidade de rodar manualmente e/ou criar jobs para fazer o agendamento da manutenção. 
-- MAGIC
-- MAGIC Mantém os dados com a melhor performance de leitura e reduz automaticamente o o custo de armazenamento garantindo a limpeza regular dos dados. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Optimize
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/delta-optimize
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/delta/optimize

-- COMMAND ----------

DESCRIBE HISTORY vendas

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

--Otimizar tabela para leitura
OPTIMIZE vendas

-- COMMAND ----------

--Otimizar tabela para leitura + Zorder by
OPTIMIZE vendas
ZORDER BY (Id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Obs: O uso de ZORDER BY melhora a performance de leitura quando você filtra os dados pelas colunas que foram ordenadas. Isso ocorre porque o Z-Ordering organiza os dados de forma que os valores próximos estejam fisicamente próximos no armazenamento, reduzindo a quantidade de dados lidos durante a consulta.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Otimização preditiva
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/optimizations/predictive-optimization
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/admin/system-tables/predictive-optimization
-- MAGIC
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/data-governance/unity-catalog/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Propriedades de Tabelas
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
-- Os comandos SET estão corretos para configuração global, mas não ativam "otimização preditiva".
-- Para ativar/desativar otimização preditiva, use propriedades específicas como 'predictive_optimization_enabled' na tabela.

-- COMMAND ----------

DESCRIBE HISTORY vendas; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - vacuum
-- MAGIC > https://learn.microsoft.com/pt-pt/azure/databricks/sql/language-manual/delta-vacuum
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY vendas; 

-- Agora para utilizar o comando Vacuum, vou deixar registrado aqui a data que foi criada a primeira tabela (2026-04-10) e a última alteração (2026-04-16), assim conseguimos ver quantos arquivos parquets foram apagados 

-- na coluna operationMetrics conseguimos ver quantos arquivos foram deletados: 
-- {"numDeletedFiles":"12","numVacuumedDirectories":"1"}


-- COMMAND ----------

vacuum vendas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como foi comentado acima, por padrão o comando VACUUM apaga os arquivos criados a mais de 7 dias atrás, porém digamos que queria apagar todos os arquivos, e manter só a última versão da tabela, conseguimos configurar as propriedades da tabela para manter o backup pela quantidade de dias que quisermos, assim como não manter versões anteriores passando um "retation"= 0 

-- COMMAND ----------

-- desativar padrao de 7 dias para Vacuum (obs: nao fazer em ambiente de produção)
ALTER TABLE vendas SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 0 hours',
  'delta.deletedFileRetentionDuration' = 'interval 0 hours'
);

-- COMMAND ----------

SHOW TBLPROPERTIES vendas;

-- COMMAND ----------

vacuum vendas RETAIN 0 HOURS

-- COMMAND ----------

DESCRIBE HISTORY vendas; 
-- {"numDeletedFiles":"2","numVacuumedDirectories":"1"}

-- COMMAND ----------

--Outro comando para fazer a mesma ação - Desativar o padrao de 7 dias para Vacuum (Global)
set spark.databricks.delta.retentionDurationCheck.enabled=false

-- COMMAND ----------

-- deletar tabelas 
drop table workspace.default.iot_devices_dt;
