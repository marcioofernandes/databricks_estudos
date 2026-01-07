-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/data-governance/unity-catalog/hive-metastore
-- MAGIC
-- MAGIC
-- MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/data-governance/unity-catalog/migrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Montar o armazenamento de objetos de nuvem no Azure Databricks
-- MAGIC
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/dbfs/mounts
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Cria um banco de dados (esquema) com o nome especificado
-- MAGIC
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema

-- COMMAND ----------

--Criando tabela padrão sem espesificar o local e Schema-DB
CREATE TABLE alunos (
  id INT,
  nome STRING,
  idade INT,
  curso STRING
);

INSERT INTO alunos (id, nome, idade, curso) VALUES
(1, 'Ana', 20, 'Matemática'),
(2, 'Bruno', 22, 'Física'),
(3, 'Carlos', 21, 'Química'),
(4, 'Diana', 23, 'Biologia'),
(5, 'Eduardo', 24, 'Ciência da Computação');


-- COMMAND ----------

drop table alunos

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/alunos'

-- COMMAND ----------

describe detail alunos;

-- COMMAND ----------

DESCRIBE EXTENDED alunos;

-- COMMAND ----------

-- DESCRIBE DETAIL e DESCRIBE EXTENDED

-- COMMAND ----------

-- criando tabela com espesificação do local
CREATE TABLE alunos_externo (
  id INT,
  nome STRING,
  idade INT,
  curso STRING
)

LOCATION 'dbfs:/mnt/teste/alunos_externo';

INSERT INTO alunos_externo (id, nome, idade, curso) VALUES
(1, 'Ana', 20, 'Matemática'),
(2, 'Bruno', 22, 'Física'),
(3, 'Carlos', 21, 'Química'),
(4, 'Diana', 23, 'Biologia'),
(5, 'Eduardo', 24, 'Ciência da Computação');

-- COMMAND ----------

drop table alunos_externo;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/teste/alunos_externo'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/teste/alunos_externo', recurse=True)

-- COMMAND ----------

describe extended alunos_externo;

-- COMMAND ----------

-- Criando Schema padrao (hive)
CREATE SCHEMA IF NOT EXISTS producao;
-- CREATE dabase IF NOT EXISTS producao;

-- COMMAND ----------

use producao; -- tudo que eu fizer abaixo vai refletir neste Banco de dados


-- Criar a tabela com 5 colunas
CREATE TABLE producao_pneus (
  id INT,
  modelo STRING,
  data_producao DATE,
  quantidade INT,
  qualidade STRING
);

-- Inserir 5 linhas de exemplo
INSERT INTO producao_pneus (id, modelo, data_producao, quantidade, qualidade) VALUES
(1, 'Modelo A', '2024-01-15', 100, 'Alta'),
(2, 'Modelo B', '2024-02-20', 150, 'Média'),
(3, 'Modelo C', '2024-03-10', 200, 'Alta'),
(4, 'Modelo D', '2024-04-05', 120, 'Baixa'),
(5, 'Modelo E', '2024-05-25', 180, 'Média');

-- COMMAND ----------

use producao;
select * from producao_pneus;

-- COMMAND ----------

USE producao;
CREATE TABLE descartes_pneus (
  id INT,
  modelo STRING,
  data_descarte DATE,
  quantidade INT,
  motivo STRING
);

INSERT INTO descartes_pneus (id, modelo, data_descarte, quantidade, motivo) VALUES
(1, 'Modelo A', '2024-06-15', 10, 'Defeito de fabricação'),
(2, 'Modelo B', '2024-07-20', 15, 'Desgaste excessivo'),
(3, 'Modelo C', '2024-08-10', 20, 'Danos durante transporte'),
(4, 'Modelo D', '2024-09-05', 12, 'Falha de qualidade'),
(5, 'Modelo E', '2024-10-25', 18, 'Outros');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Criando Schema + Metados em pasta externa

-- COMMAND ----------

-- Criando Schema Escolhendo local 
CREATE SCHEMA IF NOT EXISTS loja
LOCATION 'dbfs:/mnt/teste/lojaDB';


-- COMMAND ----------

USE loja;

CREATE TABLE IF NOT EXISTS produtos_esportivos (
  id INT,
  produto STRING,
  categoria STRING,
  data_producao DATE,
  quantidade_produzida INT
)♣♣♦☺☻☺
LOCATION 'dbfs:/mnt/teste/loja_tb_produtos_esportivos';

INSERT INTO produtos_esportivos (id, produto, categoria, data_producao, quantidade_produzida) VALUES
(1, 'Bola de Futebol', 'Esportes Coletivos', '2024-01-01', 100),
(2, 'Raquete de Tênis', 'Esportes com Raquete', '2024-02-01', 50),
(3, 'Kimono de Judô', 'Artes Marciais', '2024-03-01', 75),
(4, 'Tênis de Corrida', 'Atletismo', '2024-04-01', 150),
(5, 'Mochila de Hidratação', 'Aventura', '2024-05-01', 80);

-- COMMAND ----------

USE loja;

CREATE TABLE IF NOT EXISTS clientes (
  id INT,
  nome STRING,
  email STRING,
  data_nascimento DATE,
  cidade STRING
)
LOCATION 'dbfs:/mnt/teste/loja_tb_clientes';

INSERT INTO clientes (id, nome, email, data_nascimento, cidade) VALUES
(1, 'João Silva', 'joao.silva@example.com', '1990-01-15', 'São Paulo'),
(2, 'Maria Oliveira', 'maria.oliveira@example.com', '1985-02-20', 'Rio de Janeiro'),
(3, 'Carlos Pereira', 'carlos.pereira@example.com', '1992-03-10', 'Belo Horizonte'),
(4, 'Ana Costa', 'ana.costa@example.com', '1988-04-05', 'Porto Alegre'),
(5, 'Lucas Souza', 'lucas.souza@example.com', '1995-05-25', 'Curitiba');

-- COMMAND ----------

use loja; 
select  * from clientes

-- COMMAND ----------

use loja; 
select  * from produtos_esportivos

-- COMMAND ----------

DESCRIBE EXTENDED loja.produtos_esportivos

-- COMMAND ----------

SELECT * FROM loja.produtos_esportivos

-- COMMAND ----------

--deletando banco de dados
drop database producao cascade

-- COMMAND ----------

--deletando banco de dados
drop database loja cascade

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # delatando arquivos
-- MAGIC dbutils.fs.rm('dbfs:/mnt/teste/loja_tb_clientes/', recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # delatando arquivos
-- MAGIC dbutils.fs.rm('dbfs:/mnt/teste/loja_tb_produtos_esportivos/', recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # delatando arquivos
-- MAGIC dbutils.fs.rm('dbfs:/mnt/teste/', recurse=True)
