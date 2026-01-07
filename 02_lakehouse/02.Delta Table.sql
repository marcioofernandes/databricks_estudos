-- Databricks notebook source
--Criar tabela no Delta Lake
CREATE TABLE vendas (
    Id INT,
    nome_cliente STRING,
    data_da_compra TIMESTAMP,
    valor_compra DOUBLE
);

-- COMMAND ----------

SELECT * FROM vendas WHERE nome_cliente = 'Marta'

-- COMMAND ----------

select * from vendas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Simulando Usando em outro ambiente externo
-- MAGIC #df.write.format("delta").mode("overwrite").saveAsTable("default.vendas")

-- COMMAND ----------

--inserir dados
INSERT INTO vendas (Id, nome_cliente, data_da_compra, valor_compra) VALUES
(1, 'Edmilson', '2024-10-23 10:00:00', 150.75),
(2, 'Marta', '2024-10-23 11:00:00', 200.50),
(3, 'Lucas', '2024-10-23 12:00:00', 300.00),
(4, 'Maria', '2024-10-23 13:00:00', 250.25),
(5, 'João', '2024-10-23 14:00:00', 175.00),
(6, 'Ana', '2024-10-23 15:00:00', 225.50),
(7, 'Carlos', '2024-10-23 16:00:00', 275.75),
(8, 'Fernanda', '2024-10-23 17:00:00', 325.00),
(9, 'Paulo', '2024-10-23 18:00:00', 350.25),
(10, 'Beatriz', '2024-10-23 19:00:00', 400.50);

-- COMMAND ----------

--DESCRIBE DETAIL Ver informações avançadas local , logs Arquivos de metadados etc
DESCRIBE DETAIL vendas


-- COMMAND ----------

-- MAGIC %md
-- MAGIC --Verificar Arquivos de Logs como informado em aula anterior

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas/_delta_log/''

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head 'dbfs:/user/hive/warehouse/vendas/_delta_log/00000000000000000003.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Logs Significados
-- MAGIC
-- MAGIC Os arquivos e diretórios que você vê no diretório _delta_log são parte do log de transações do Delta Lake. Este log é essencial para garantir as propriedades ACID (Atomicidade, Consistência, Isolamento e Durabilidade) das tabelas Delta. Aqui está uma breve explicação de cada tipo de arquivo:
-- MAGIC
-- MAGIC **00000000000000000000.json e 00000000000000000001.json:**
-- MAGIC Estes são arquivos de log de transações. Cada arquivo JSON contém um conjunto de ações que foram aplicadas à tabela Delta, como adições, remoções ou atualizações de arquivos de dados. Eles são numerados sequencialmente para manter a ordem das transações.
-- MAGIC
-- MAGIC **00000000000000000000.crc e 00000000000000000001.crc:**
-- MAGIC Estes são arquivos de checksum (CRC - Cyclic Redundancy Check) que garantem a integridade dos arquivos de log de transações correspondentes. Eles ajudam a detectar qualquer corrupção nos arquivos de log.
-- MAGIC
-- MAGIC **__tmp_path_dir/:**
-- MAGIC Este é um diretório temporário usado durante operações de escrita. Ele pode conter arquivos temporários que são movidos ou renomeados após a conclusão da operação.
-- MAGIC
-- MAGIC **_commits/:**
-- MAGIC Este diretório pode conter informações adicionais sobre commits, como metadados ou arquivos auxiliares usados para gerenciar as transações.

-- COMMAND ----------

--fazer um Update na tabela
UPDATE vendas
SET valor_compra = 250.50
WHERE nome_cliente = 'Edmilson';

-- COMMAND ----------

select * from vendas

-- COMMAND ----------

--Fazer outro insert na tabela
INSERT INTO vendas (Id, nome_cliente, data_da_compra, valor_compra) VALUES
(11, 'Pedro', '2024-10-24 10:00:00', 180.75),
(12, 'Juliana', '2024-10-24 11:00:00', 220.50),
(13, 'Roberto', '2024-10-24 12:00:00', 310.00),
(14, 'Clara', '2024-10-24 13:00:00', 260.25),
(15, 'Sofia', '2024-10-24 14:00:00', 195.00);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como ver o histórico de transações?

-- COMMAND ----------

--DESCRIBE HISTORY - ver Histórico de alterações
DESCRIBE HISTORY vendas

-- COMMAND ----------

--VERSION AS OF  ver uma versao da tabela 
SELECT * FROM vendas VERSION AS OF 2

-- COMMAND ----------

-- Como salvar a versao que voce quer na tabela?
-- opção 1 criando uma nova tabela
-- opção 2 OVERWRITE TABLE (substituir os dados da tabela)
-- Criar uma View

-- COMMAND ----------

--VERSION AS OF  Criar uma nova tabela com versao 2
CREATE TABLE vendas_version_2 AS
SELECT * FROM vendas VERSION AS OF 2

-- COMMAND ----------

-- substitui toda a tabela pelos novos dados 
INSERT OVERWRITE TABLE vendas
SELECT * FROM vendas VERSION AS OF 2;

-- COMMAND ----------

select * from vendas

-- COMMAND ----------

-- Criar uma View com uma versao da tabela  
CREATE OR REPLACE VIEW vendas_view AS
SELECT * FROM vendas VERSION AS OF 2;

-- COMMAND ----------


SELECT * FROM vendas_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Formas Avançadas de recuperação de versão

-- COMMAND ----------

--DESCRIBE HISTORY - ver Histórico de alterações
DESCRIBE HISTORY vendas

-- COMMAND ----------

select * from vendas version as of 3

-- COMMAND ----------

select * from vendas TIMESTAMP AS OF '2024-10-24T01:25:01.000+00:00'

-- COMMAND ----------

SELECT  * FROM vendas @V3

-- COMMAND ----------

DELETE FROM vendas

-- COMMAND ----------

SELECT * FROM vendas

-- COMMAND ----------

-- RESTAURAR DADOS - VERSÃO
RESTORE TABLE vendas VERSION AS OF 4

-- COMMAND ----------

SELECT  * FROM vendas
