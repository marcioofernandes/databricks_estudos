-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Particionamento

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Documentação Referencia
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/tables/external-partition-discovery
-- MAGIC
-- MAGIC
-- MAGIC O Databricks recomenda habilitar o registro em log de metadados de partição para melhorar as velocidades de leitura e o desempenho da consulta para tabelas externas do Catálogo do Unity com partições.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS logistica;


-- COMMAND ----------

USE logistica; -- tudo que eu fizer abaixo vai refletir neste Banco de dados

CREATE OR REPLACE TABLE vendas_pneus (
  id INT,
  modelo STRING,
  data_producao DATE,
  quantidade INT,
  qualidade STRING,
  estado STRING
)
PARTITIONED BY (estado)
LOCATION '/mnt/logistica/vendas_pneus'
;

-- COMMAND ----------

-- Inserir 5 linhas de exemplo
INSERT INTO logistica.vendas_pneus (id, modelo, data_producao, quantidade, qualidade, estado) VALUES
(6, 'Modelo A', '2024-06-01', 110, 'Alta', 'SP'),
(7, 'Modelo B', '2024-06-02', 130, 'Média', 'RJ'),
(8, 'Modelo C', '2024-06-03', 140, 'Alta', 'MG'),
(9, 'Modelo D', '2024-06-04', 150, 'Baixa', 'BA'),
(10, 'Modelo E', '2024-06-05', 160, 'Média', 'RS'),
(11, 'Modelo A', '2024-06-06', 170, 'Alta', 'PR'),
(12, 'Modelo B', '2024-06-07', 180, 'Média', 'SC'),
(13, 'Modelo C', '2024-06-08', 190, 'Alta', 'PE'),
(14, 'Modelo D', '2024-06-09', 200, 'Baixa', 'CE'),
(15, 'Modelo E', '2024-06-10', 210, 'Média', 'GO'),
(16, 'Modelo A', '2024-06-11', 220, 'Alta', 'AM'),
(17, 'Modelo B', '2024-06-12', 230, 'Média', 'PA'),
(18, 'Modelo C', '2024-06-13', 240, 'Alta', 'MT'),
(19, 'Modelo D', '2024-06-14', 250, 'Baixa', 'MS'),
(20, 'Modelo E', '2024-06-15', 260, 'Média', 'DF'),
(21, 'Modelo A', '2024-06-16', 270, 'Alta', 'ES'),
(22, 'Modelo B', '2024-06-17', 280, 'Média', 'PB'),
(23, 'Modelo C', '2024-06-18', 290, 'Alta', 'RN'),
(24, 'Modelo D', '2024-06-19', 300, 'Baixa', 'AL'),
(25, 'Modelo E', '2024-06-20', 310, 'Média', 'SE'),
(26, 'Modelo A', '2024-06-21', 320, 'Alta', 'PI'),
(27, 'Modelo B', '2024-06-22', 330, 'Média', 'MA'),
(28, 'Modelo C', '2024-06-23', 340, 'Alta', 'TO'),
(29, 'Modelo D', '2024-06-24', 350, 'Baixa', 'RO'),
(30, 'Modelo E', '2024-06-25', 360, 'Média', 'AC'),
(31, 'Modelo A', '2024-06-26', 370, 'Alta', 'AP'),
(32, 'Modelo B', '2024-06-27', 380, 'Média', 'RR'),
(33, 'Modelo C', '2024-06-28', 390, 'Alta', 'SP'),
(34, 'Modelo D', '2024-06-29', 400, 'Baixa', 'RJ'),
(35, 'Modelo E', '2024-06-30', 410, 'Média', 'MG'),
(36, 'Modelo A', '2024-07-01', 420, 'Alta', 'BA'),
(37, 'Modelo B', '2024-07-02', 430, 'Média', 'RS'),
(38, 'Modelo C', '2024-07-03', 440, 'Alta', 'PR'),
(39, 'Modelo D', '2024-07-04', 450, 'Baixa', 'SC'),
(40, 'Modelo E', '2024-07-05', 460, 'Média', 'PE'),
(41, 'Modelo A', '2024-07-06', 470, 'Alta', 'CE'),
(42, 'Modelo B', '2024-07-07', 480, 'Média', 'GO'),
(43, 'Modelo C', '2024-07-08', 490, 'Alta', 'AM'),
(44, 'Modelo D', '2024-07-09', 500, 'Baixa', 'PA'),
(45, 'Modelo E', '2024-07-10', 510, 'Média', 'MT'),
(46, 'Modelo A', '2024-07-11', 520, 'Alta', 'MS'),
(47, 'Modelo B', '2024-07-12', 530, 'Média', 'DF'),
(48, 'Modelo C', '2024-07-13', 540, 'Alta', 'ES'),
(49, 'Modelo D', '2024-07-14', 550, 'Baixa', 'PB'),
(50, 'Modelo E', '2024-07-15', 560, 'Média', 'RN');

-- COMMAND ----------

SELECT * FROM logistica.vendas_pneus

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/logistica/vendas_pneus'

-- COMMAND ----------

SELECT * FROM logistica.vendas_pneus
WHERE estado = 'MG'

-- COMMAND ----------

--ver particioes de uma tabela existente
show partitions logistica.vendas_pneus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE AS SELECT (CTAS) -- CREATE TABLE [USING]
-- MAGIC
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using#as

-- COMMAND ----------

select * from logistica.vendas_pneus

-- COMMAND ----------

select * from logistica.vendas_pneus
where estado = 'MG'

-- COMMAND ----------

-- Criando CTA
create or replace table logistica.cta_vendas_pneus
as 
select * from logistica.vendas_pneus
where estado = 'MG'

-- COMMAND ----------

select * from logistica.cta_vendas_pneus

-- COMMAND ----------

INSERT INTO logistica.vendas_pneus (id, modelo, data_producao, quantidade, qualidade, estado) VALUES
(6, 'Modelo A', '2024-06-01', 310, 'Alta', 'MG')

-- COMMAND ----------

-- Criando CTA add configurações 
create or replace table logistica.cta_configuracoes_vendas_pneus

COMMENT   'Cta de teste venda pneus'
PARTITIONED BY (UF)
LOCATION '/mnt/dados/logistica/cta_configuracoes_vendas_pneus'

as 

select 
  id as identificador
  ,quantidade
  ,data_producao
  ,estado as UF
   from logistica.vendas_pneus
where estado = 'MG'

-- COMMAND ----------

select * from logistica.cta_configuracoes_vendas_pneus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE IF NOT EXISTS e CREATE OR REPLACE TABLE não são a mesma coisa.
-- MAGIC
-- MAGIC - **CREATE TABLE IF NOT EXISTS** cria a tabela apenas se ela não existir. Se a tabela já existir, o comando não faz nada.
-- MAGIC - **CREATE OR REPLACE TABLE** cria a tabela, substituindo qualquer tabela existente com o mesmo nome. Isso significa que a tabela existente será descartada e uma nova tabela será criada.

-- COMMAND ----------

SHOW TABLES;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sobre Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Referencias
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/views/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Comparação de Views 
-- MAGIC Uma exibição ou View é o resultado de uma consulta em uma ou mais tabelas e exibições
-- MAGIC
-- MAGIC ### Views Armazenadas (Stored Views)
-- MAGIC - **Persistidas no Banco de Dados**: Views armazenadas são salvas no banco de dados e persistem entre sessões.
-- MAGIC - **Removidas**: Essas views só podem ser removidas usando o comando `DROP VIEW`.
-- MAGIC - **Criação**: Use a instrução `CREATE VIEW` para criar views armazenadas.
-- MAGIC
-- MAGIC ### Views Temporárias (Temp Views)
-- MAGIC - **Escopo da Sessão**: Views temporárias estão disponíveis apenas na sessão atual.
-- MAGIC - **Removidas**: Essas views são removidas quando a sessão termina.
-- MAGIC - **Criação**: Use a instrução `CREATE TEMP VIEW` para criar views temporárias.
-- MAGIC
-- MAGIC ### Views Temporárias Globais (Global Temp Views)
-- MAGIC - **Escopo do Cluster**: Views temporárias globais estão disponíveis em todas as sessões no mesmo cluster.
-- MAGIC - **Removidas**: Essas views são removidas quando o cluster é reiniciado.
-- MAGIC - **Criação**: Use a instrução `CREATE GLOBAL TEMP VIEW` para criar views temporárias globais.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Praticando
-- MAGIC

-- COMMAND ----------

-- Criar o banco de dados Sej Corridas
CREATE SCHEMA IF NOT EXISTS corridas;

-- Usar o banco de dados Sej Corridas
USE corridas;

-- Criar a tabela de corridas de Fórmula 1
CREATE OR REPLACE TABLE corridas_f1 (
  id INT,
  nome STRING,
  piloto STRING,
  local_corrida STRING,
  data_corrida DATE,
  vencedor STRING
)
;

-- Inserir alguns exemplos de corridas de Fórmula 1
INSERT INTO corridas.corridas_f1 (id, nome, piloto, local_corrida, data_corrida, vencedor) VALUES
(1, 'Grande Prêmio do Bahrein', 'Charles Leclerc', 'Bahrein', '2023-03-05', 'Max Verstappen'),
(2, 'Grande Prêmio da Arábia Saudita', 'Sergio Perez', 'Arábia Saudita', '2023-03-19', 'Sergio Perez'),
(3, 'Grande Prêmio da Austrália', 'Lewis Hamilton', 'Austrália', '2023-04-02', 'Max Verstappen'),
(4, 'Grande Prêmio da Itália', 'Carlos Sainz', 'Itália', '2023-09-03', 'Max Verstappen'),
(5, 'Grande Prêmio de Abu Dhabi', 'Fernando Alonso', 'Abu Dhabi', '2023-11-26', 'Max Verstappen'),
(6, 'Grande Prêmio do Bahrein', 'Charles Leclerc', 'Bahrein', '2024-03-03', 'Max Verstappen'),
(7, 'Grande Prêmio da Arábia Saudita', 'Sergio Perez', 'Arábia Saudita', '2024-03-17', 'Sergio Perez'),
(8, 'Grande Prêmio da Austrália', 'Lewis Hamilton', 'Austrália', '2024-03-31', 'Max Verstappen'),
(9, 'Grande Prêmio da Itália', 'Carlos Sainz', 'Itália', '2024-09-01', 'Max Verstappen'),
(10, 'Grande Prêmio de Abu Dhabi', 'Fernando Alonso', 'Abu Dhabi', '2024-11-24', 'Max Verstappen'),
(11, 'Grande Prêmio de Mônaco', 'Charles Leclerc', 'Mônaco', '2023-05-28', 'Max Verstappen'),
(12, 'Grande Prêmio do Canadá', 'Lewis Hamilton', 'Canadá', '2023-06-18', 'Max Verstappen'),
(13, 'Grande Prêmio da Grã-Bretanha', 'Lando Norris', 'Grã-Bretanha', '2023-07-09', 'Max Verstappen'),
(14, 'Grande Prêmio da Hungria', 'George Russell', 'Hungria', '2023-07-23', 'Max Verstappen'),
(15, 'Grande Prêmio da Bélgica', 'Sergio Perez', 'Bélgica', '2023-07-30', 'Max Verstappen'),
(16, 'Grande Prêmio da Holanda', 'Max Verstappen', 'Holanda', '2023-08-27', 'Max Verstappen'),
(17, 'Grande Prêmio de Singapura', 'Carlos Sainz', 'Singapura', '2023-09-17', 'Carlos Sainz'),
(18, 'Grande Prêmio do Japão', 'Charles Leclerc', 'Japão', '2023-09-24', 'Max Verstappen'),
(19, 'Grande Prêmio do Catar', 'Lewis Hamilton', 'Catar', '2023-10-08', 'Max Verstappen'),
(20, 'Grande Prêmio dos Estados Unidos', 'Sergio Perez', 'Estados Unidos', '2023-10-22', 'Max Verstappen'),
(21, 'Grande Prêmio do México', 'Sergio Perez', 'México', '2023-10-29', 'Max Verstappen'),
(22, 'Grande Prêmio do Brasil', 'Lewis Hamilton', 'Brasil', '2023-11-12', 'Max Verstappen');

-- COMMAND ----------

select * from corridas.corridas_f1

-- COMMAND ----------

CREATE OR REPLACE VIEW View_corridas_f1 
AS
 --consulta para gerar a View
select * from corridas.corridas_f1


-- COMMAND ----------

select * from View_corridas_f1 

-- COMMAND ----------

show  tables  in corridas

-- COMMAND ----------

-- criar temp view
CREATE OR REPLACE TEMP VIEW TEMP_View_corridas_f1 

AS

select * from corridas.corridas_f1

-- COMMAND ----------

SELECT * FROM TEMP_View_corridas_f1 

-- COMMAND ----------

SHOW TABLES IN corridas

-- COMMAND ----------

-- criar GLOBAL temp view
CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_View_corridas_f1 

AS

select * from corridas.corridas_f1

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

-- insertt teste 
INSERT INTO corridas.corridas_f1 (id, nome, piloto, local_corrida, data_corrida, vencedor) VALUES
(1, 'Grande Prêmio do Bahrein', 'Charles Leclerc', 'Bahrein', '2024-03-05', 'Edmilson Alves')
