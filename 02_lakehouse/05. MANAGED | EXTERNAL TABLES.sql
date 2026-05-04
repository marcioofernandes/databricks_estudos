-- Databricks notebook source
-- MAGIC %md
-- MAGIC No Databricks as tabelas podem ser categorizadas principalmente por como tabelas gerenciadas (MANAGED TABLES) e tabelas externas (EXTERNAL TABLES), dependendo do nível de controle que a plataforma exerce sobre os dados fisícos. 
-- MAGIC
-- MAGIC ### Managed Tables
-- MAGIC
-- MAGIC É o tipo de tabela padrão criada dentro do ambiente do Databricks. Nessas tabelas o Databricks por meio do UC tem total responsabilidade pelo ciclo de vida da tabela, gerenciando tanto os metadados quanto os próprios arquivos de dados físicos.
-- MAGIC
-- MAGIC **Armazenamento**: Os arquivos de dados físicos dessas tabelas são salvos diretamente nos locais de armazenamento em nuvem já configurado e gerenciados associados ao schema ou catálogo ao qual a tabela pertence 
-- MAGIC
-- MAGIC **Quando usar**: São a escolha padrão recomendada para a criação de novas tabelas que requerem alto desempenho otimizado pela plataforma 
-- MAGIC
-- MAGIC ### External Tables 
-- MAGIC
-- MAGIC São tabelas onde os dados ficam armazenados em outros locais ou sistemas externos de armazenamento em nuvem (ex: AWS, AZURE e etc) geridos pelo próprio usuário. 
-- MAGIC
-- MAGIC **armazenamento** Neste modelo o Databricks gerencia apenas os metadados (como o schema da tabela, nome das colunas e as permissões de acesso ao UC). Ele não move nem controla os arquivos fisícos. Para criar as tabelas external, o usuário deve já ter os dados armazenados em um local externo (usando as permissões de external location no UC). 
-- MAGIC
-- MAGIC **Quando usar**: São ideais quando você precisa governar e consultar dados estruturados (como uma pasta cheia de arquivos parquets em um s3) usando as ferramentas do Databricks e o Unity Catalog, mas sem precisar mover ou reescrever os dados inicialmente para o armazenamento do databricks.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Aqui há duas documentações mais antigas de quando era hive-matastore, a própria Databricks diz que está defasado, mas é bom manter caso trabalhe em algum sistema herdado 
-- MAGIC
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

-- Como vimos anteriormente, ao criarmos tabelas sem especificar schema e catalog, elas são geradas no workspace.default 
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

-- MAGIC %md
-- MAGIC ### LISTA DE COMANDOS PARA PEGAR INFORMAÇÕES DA TABELA 
-- MAGIC
-- MAGIC **DESCRIBE DETAIL:** traz informações resumidas da tabela, como nome, formato, id, localização, etc. 
-- MAGIC
-- MAGIC **DESCRIBE EXTENDED**: pareciso com o describe detail, porém com muito mais informações sobre a tabela, como colunas, data_types, data de criação, owner, se ela é gerenciada, etc...
-- MAGIC
-- MAGIC **DESCRIBE HISTORY**: Descreve as atualiazações que a tabela recebeu, como write, create, delete, etc (time-travel)
-- MAGIC
-- MAGIC **DESCRIBE TABLE**: traz informações de colunas, data_type e comentarios  

-- COMMAND ----------

describe table alunos; 

-- COMMAND ----------

describe detail alunos;

-- COMMAND ----------

DESCRIBE EXTENDED alunos;

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA default;

-- COMMAND ----------

drop table alunos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para criar tableas externas passamos o LOCATION de onde a tabela será criada, não consigo reproduzir os comando abaixo pois o UC não permite que eu utilize o hive e processos de mount que eram usados em sistemas mais antigos do Databricks. (Assim como as tabelas criadas pelo UC não possui location, pois quem controla esse processo é o Databricks)

-- COMMAND ----------

-- criando external tables 
CREATE TABLE alunos_externo (
  id INT,
  nome STRING,
  idade INT,
  curso STRING
)

LOCATION 'dbfs:/mnt/teste/alunos';
-- Aqui funciona caso queira criar um tabela usando o S3 por exemplo, daria para passar nesse formato: 
-- LOCATION s3://your-bucket/path/alunos_externo/
INSERT INTO alunos_externo (id, nome, idade, curso) VALUES
(1, 'Ana', 20, 'Matemática'),
(2, 'Bruno', 22, 'Física'),
(3, 'Carlos', 21, 'Química'),
(4, 'Diana', 23, 'Biologia'),
(5, 'Eduardo', 24, 'Ciência da Computação');

-- COMMAND ----------

drop table alunos_catalog;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Remover arquivos de pastas external
-- MAGIC dbutils.fs.rm('s3://your-bucket/path/alunos_externo/', recurse=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CRIANDO E ESCOLHENDO CATALOGS / SCHEMA
-- MAGIC
-- MAGIC Normalmente existe mais de uma maneira de criar qualquer coisa no Databricks, uma delas é usando a interface de UI e a outra através de código, com o tempo você vai se acostumando com a interface da plataforma e escolhe sua maneira preferida de trabalhar 
-- MAGIC
-- MAGIC Por exemplo, para criar um catalog novo, você pode ir na aba esquerda, clicar em catalog, no canto superior direito terá uma opção "Create" que abre uma caixa de seleção, que contém catalog, volume, connection e etc
-- MAGIC
-- MAGIC Aqui no notebook iremos mostrar a opção através de SQL mas lembre-se, a maioria das coisas que você faz em SQL, pode escolher fazer em python, pyspark, Spark.sql e etc 
-- MAGIC

-- COMMAND ----------

-- O mesmo comando que você cria o Catalog pode ser usado para criar um Schema
CREATE CATALOG IF NOT EXISTS teste; 

-- COMMAND ----------

CREATE TABLE teste.default.teste (
  id INT, 
  nome string, 
  qualquer string
)

-- COMMAND ----------

-- como meu catalogo padrão é o workspace, se eu tentar rodar essa query agora, mesmo tendo já criado a tabela teste, ela vai falhar 
-- Após ativar a célula abaixo USE CATALOG torne a rodar essa célula que vai funcionar 
select * from teste;

-- COMMAND ----------

-- O comando USE CATALOG serve para que você ative durante esse desenvolvimento o catalog escolhido como padrão, ajuda caso não queria escrever o caminho completo da tabela na query, mas é um comando opcional. 
USE CATALOG teste;

-- COMMAND ----------

-- Criando Schema 
CREATE SCHEMA IF NOT EXISTS producao;
USE SCHEMA producao;
-- tudo que eu fizer abaixo a partir do momento que rodar essa célula, vai refletir no caminho teste.producao 
-- Pois eu ativei como padrão durante esse desenvolvimento o USE CATALOG e o SCHEMA 

-- COMMAND ----------


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

select * from producao_pneus;

-- COMMAND ----------

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
-- MAGIC Criando Schema + Metadados em pasta externa 

-- COMMAND ----------

-- Criando Schema Escolhendo local 
CREATE SCHEMA IF NOT EXISTS loja
MANAGED LOCATION 's3://your-bucket/arquivos-aula/teste/';


-- COMMAND ----------

USE loja;

CREATE TABLE IF NOT EXISTS produtos_esportivos (
  id INT,
  produto STRING,
  categoria STRING,
  data_producao DATE,
  quantidade_produzida INT
)

LOCATION 's3://your-bucket/arquivos-aula/teste/';

INSERT INTO produtos_esportivos (id, produto, categoria, data_producao, quantidade_produzida) VALUES
(1, 'Bola de Futebol', 'Esportes Coletivos', '2024-01-01', 100),
(2, 'Raquete de Tênis', 'Esportes com Raquete', '2024-02-01', 50),
(3, 'Kimono de Judô', 'Artes Marciais', '2024-03-01', 75),
(4, 'Tênis de Corrida', 'Atletismo', '2024-04-01', 150),
(5, 'Mochila de Hidratação', 'Aventura', '2024-05-01', 80);

-- COMMAND ----------

use loja; 
select  * from produtos_esportivos

-- COMMAND ----------

DESCRIBE EXTENDED loja.produtos_esportivos

-- COMMAND ----------

SELECT * FROM loja.produtos_esportivos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### COMANDO PARA DELETAR CATALOG / SCHEMA 
-- MAGIC e arquivos ou tabelas contidas dentro deles, sem precisar remover 1 a 1 

-- COMMAND ----------

--Deleta o schema junto com as duas tabelas dentro dele 
drop SCHEMA producao cascade

-- COMMAND ----------

--Deleta o catalog e todos schemas criados dentro dele 
drop catalog teste cascade

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # delatando arquivos de dentro de pastas externas 
-- MAGIC dbutils.fs.rm('dbfs:/mnt/teste/loja_tb_clientes/', recurse=True)
