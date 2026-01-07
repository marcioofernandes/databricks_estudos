# Databricks notebook source
# MAGIC %md
# MAGIC Documentação Databricks
# MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/
# MAGIC
# MAGIC O Que é Delta Lake
# MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/delta/
# MAGIC
# MAGIC Curiosidades
# MAGIC https://www.databricks.com/br/glossary/data-lakehouse
# MAGIC
# MAGIC O que são garantias ACID no Azure Databricks?
# MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/lakehouse/acid
# MAGIC
# MAGIC O conceito de "lakehouse" refere-se a uma arquitetura de dados que combina elementos de data lakes e data warehouses. Ele oferece a flexibilidade e a escalabilidade de um data lake com a estrutura e as capacidades de gerenciamento de dados de um data warehouse. A arquitetura lakehouse permite que as organizações armazenem dados estruturados e não estruturados em um único repositório, suportando tanto análises em tempo real quanto análises históricas.
# MAGIC
# MAGIC Principais características de um lakehouse:
# MAGIC
# MAGIC **Armazenamento unificado**: Suporta dados estruturados, semiestruturados e não estruturados.
# MAGIC
# MAGIC **Processamento de dados**: Capacidade de realizar ETL (Extração, Transformação e Carga) e ELT (Extração, Carga e Transformação).
# MAGIC
# MAGIC **Gerenciamento de dados**: Oferece governança, segurança e controle de acesso.
# MAGIC
# MAGIC **Desempenho**: Otimizado para consultas analíticas e processamento de grandes volumes de dados.
# MAGIC
# MAGIC ACID significa atomicidade, consistência, isolamento e durabilidade.
# MAGIC
# MAGIC **Atomicidade** significa que todas as transações são bem-sucedidas ou falham completamente.
# MAGIC
# MAGIC As garantias de **consistência** referem-se à forma como um determinado estado dos dados é observado por operações simultâneas.
# MAGIC
# MAGIC **Isolamento** refere-se a como operações simultâneas potencialmente entram em conflito umas com as outras.
# MAGIC
# MAGIC A **durabilidade** significa que as alterações autorizadas são permanentes.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![.](https://www.databricks.com/wp-content/uploads/2020/01/data-lakehouse-new.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Logs

# COMMAND ----------

# MAGIC %md
# MAGIC Logs 
# MAGIC https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html

# COMMAND ----------

# MAGIC %md
# MAGIC Dividindo as transações em confirmações atômicas
# MAGIC Sempre que um usuário executa uma operação para modificar uma tabela (como INSERT, UPDATE ou DELETE), o Delta Lake divide essa operação em uma série de etapas discretas compostas por uma ou mais das ações abaixo.

# COMMAND ----------

# MAGIC %md
# MAGIC Quando um usuário cria uma tabela do Delta Lake, o log de transações dessa tabela é criado automaticamente no subdiretório _delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Log Inicial padrao
# MAGIC ![.](https://www.databricks.com/wp-content/uploads/2019/08/image7.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### varias ações 
# MAGIC ![](https://www.databricks.com/wp-content/uploads/2019/08/image6-1.png)
