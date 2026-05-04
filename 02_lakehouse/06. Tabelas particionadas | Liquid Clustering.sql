-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Particionamento de tabelas 
-- MAGIC
-- MAGIC Existe duas formas de fazer o particionamento das tabelas no Databricks. Primeiro iremos explorar o formato um pouco mais antigo, conhecido como particionamento estilo Hive, que é uma técnica que divide e organiza fisicamente os arquivos de dados em subdiretórios distintos com base nos valores de uma ou mais coluna. Podemos pensar nisso em um projeto onde temos uma tabela "vendas" que os metadados ficam armazenados em um S3, por se tratar de uma tabela onde contém muitos registros, caso precise procurar uma venda especifica, pode ser muito custoso fazer uma consulta na tabela. 
-- MAGIC
-- MAGIC O particionamento vem para resolver esse problema, digamos que decida particionar a tabela por ano, mês, dia (a estrutura de pasta no S3 ficaria Vendas > Ano > mês > dia) e você precisa de um dado de uma venda que aconteceu no dia 2026/01/01, a consulta vai ignorar todos outros arquivos da tabela, e processar apenas o arquivo que estão dentro desta partição. 
-- MAGIC
-- MAGIC **Quando usar?**: O Databricks recomenda 3 regras de ouro para saber se deve ou não particionar suas tabelas. 
-- MAGIC
-- MAGIC 1. **Apenas tabelas gigantes** Você só deve particionar tabelas se o tamanho total dela for maior que 1 Terabyte. Para tabelas menores, a recomendação oficial é não particionar, basta usar o comando **OPTIMIZE** para resolver performance. 
-- MAGIC
-- MAGIC 2. **Tamanho de partição robusto** Certifique se de escolher colunas que a quantidade de dados que cairá dentro de cada partição será de pelo menos 1GB. 
-- MAGIC
-- MAGIC 3. **Evite colunas de alta cardinalidade**: Nunca utilize colunas que possui muitos valores únicos distintos, o ideal é escolher colunas como data, ou categoria por exemplo. 
-- MAGIC
-- MAGIC **Perigos de superparticionamento (Overpartitioning)**
-- MAGIC
-- MAGIC Se as regras acima não forem seguidas, a tabela sofrerá overpartitioning, que é uma das maiores causas de lentidão no Databricks, por que gera diversos arquivos pequenos obrigando a plataforma a gastar tempo e recurso para varrer todos os pequenos arquivos para selecionar o correto. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Documentação Referencia
-- MAGIC https://learn.microsoft.com/pt-br/azure/databricks/tables/external-partition-discovery
-- MAGIC
-- MAGIC
-- MAGIC O Databricks recomenda habilitar o registro em log de metadados de partição para melhorar as velocidades de leitura e o desempenho da consulta para tabelas externas do Catálogo do Unity com partições.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### quando usar a versão Hive de particionamento? 
-- MAGIC
-- MAGIC Atualmente há versões mais atualizadas para trabalhar com particionamento, iremos abordar elas mais abaixo neste notebook, mas a opção PARTITIONED BY ainda tem seu caso de uso. 
-- MAGIC
-- MAGIC Por exemplo, Se os seus dados já residem em ambiente externo por exemplo o S3 e estão organizados fisicamente no formato de diretório particionado (particionamento no estilo Hive, com pastas como ano=2023/mes=10) e você quer transformar isso em uma tabela externa dentro do Databricks ou converte-la em formato Delta (usando ferramenta como CONVERT TO DELTA), vodê DEVE usar o PARTITIONED BY. 
-- MAGIC
-- MAGIC O Databricks exige que você informe qual era o particionamento original (ex: PARTITIONED BY (data DATE)) para conseguir ler e mapear a estrutura de diretórios que já existe no seu bucket S3. 

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS logistica;


-- COMMAND ----------

USE logistica; -- tudo que eu fizer abaixo vai refletir neste schema

CREATE OR REPLACE TABLE vendas_pneus (
  id INT,
  modelo STRING,
  data_producao DATE,
  quantidade INT,
  qualidade STRING,
  estado STRING
)
PARTITIONED BY (estado)
-- LOCATION '/mnt/logistica/vendas_pneus' (Aqui é para salvar a tabela em um ambiente externo)
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

SELECT * FROM logistica.vendas_pneus
WHERE estado = 'MG'

-- COMMAND ----------

-- O comando para ver as partições da tabela é 
SHOW PARTITIONS logistica.vendas_pneus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### A alternativa moderna: LIQUID CLUSTERING
-- MAGIC
-- MAGIC Para resolver o problema de escolher a partição ideial e evitar o superparticionamento, o DELTA LAKE 3.0 introduziu o Liquid Clustering. Ele foi projetado para substituir o particionamento tradicional do Hive (PARTITIONED BY) e a necessidade de usar o comando ZORDER de forma independente. 
-- MAGIC
-- MAGIC Em vez de criar uma base de pastas particionadas por colunas, o Liquid Clustering ajusta dinamicamente a forma como os arquivos de dados são agrupados em nível de arquivo para maximizar a velocidade de leitura e gravacão. 
-- MAGIC
-- MAGIC O funcionamento do Liquid Clustering baseia-se em algoritmos matemáticos complexos chamados curvas de preenchimento de espaço (space-filling curves), semelhante o Z-ordering porém otimizado, na prática ele faz: 
-- MAGIC
-- MAGIC 1. **Agrupamento antecipado (Eager Clustering):** Quando você insere novos dados (sob o limit padrão de 512GB), a plataforma automaticamente agrupa os dados durante a operação de gravação. 
-- MAGIC
-- MAGIC 2. **Compactação continua (OPTIMIZE)**: diferente do modelo anterior que exigia parâmetros complexos, ao executar o comando OPTIMIZE na tabela, a plataforma verifica se os dados sofreram alterações e junta ou divide os arquivos para garantir o agrupamento perfeito, gerando arquivos com tamanhos consistentes. 
-- MAGIC
-- MAGIC ##### Principais caracteristicas e vantagens 
-- MAGIC
-- MAGIC 1. **Flexibilidade total de chaves**: O liquid clustering é ativado na criação da tabela com o comando `CLUSTER BY` E a grande vantagem é que se o perfil de consultas do seu negócio mudar, você pode usar um comando `ALTER TABLE` para trocar as colunas de clusterização (ou até mesmo removê-las usando o `NONE`) e a mudança será aplicanda nas próximas compactações sem a necessidade de reescrever a tabela inteira. 
-- MAGIC
-- MAGIC 2. **Automação inteligente:** Você também pode usar o comando `CLUSTER BY AUTO`, com isso o Databricks analisa as cargas de trabalhos e escolhe ou atualiza as chaves de clusterização ideiais para acelerar as consultas. 
-- MAGIC
-- MAGIC 3. **Concorrência a nível de linhas (Row-level Concurrency)**: Como ele elimina a estrutura de pastas fisícas você ganha a capacidade de rodar as modicações lado a lado com o OPTIMIZE sem causar bloqueios ou falhas de concorrência. Só haverá conflitos se tentar rodar dois processos ao mesmo tempo com a mesma linha de dados. 
-- MAGIC
-- MAGIC 4. **Elimina o overpartitioning**: Elimina o risco de super parcicionamento criando diversos arquivos pequenos, garantindo que todos os arquivos particionados tenham um mesmo tamanho equilibrado. 
-- MAGIC
-- MAGIC ##### Limitações e Regras de uso 
-- MAGIC
-- MAGIC - O Liquid Clustering não pode existir junto com o particionamento tradicional, ou seja, não dá para usar o CLUSTER BY e o PARTITIONED BY na mesma tabela 
-- MAGIC
-- MAGIC - Você não precisará e nem conseguirá rodar o comando ZORDER BY junto com o OPTIMIZE em um tabela configurada com o Liquid cluster pois ele já é nativo 
-- MAGIC
-- MAGIC - Por ser uma tecnologia mais nova do protocolo Delta, habilitá-lo exige leitores e gravadores compatíveis (Writer version 7 e Reader version 3). Sistemas clientes muito antigos e desatualizados que consultem o seu armazenamento na nuvem podem não conseguir ler essas tabelas

-- COMMAND ----------

-- aplicando o cluster by na prática 

CREATE OR REPLACE TABLE vendas_pneus_cluster (
  id INT,
  modelo STRING,
  data_producao DATE,
  quantidade INT,
  qualidade STRING,
  estado STRING
)
CLUSTER BY (estado)


-- COMMAND ----------

-- Inserir 5 linhas de exemplo
INSERT INTO logistica.vendas_pneus_cluster (id, modelo, data_producao, quantidade, qualidade, estado) VALUES
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

-- O comando SHOW PARTITIONS não funciona para tabelas com CLUSTER BY, porém consegues ver a coluna particionada da tabela pelo comando DESCRIBE TABLE 
DESCRIBE TABLE logistica.vendas_pneus_cluster;

-- COMMAND ----------

-- Alterando a chave do clustering 
ALTER TABLE logistica.vendas_pneus_cluster
CLUSTER BY (qualidade) 
-- verifique a alteração da coluna na célula acima
-- Nota: a tabela pode ser clusterizado por mais de uma coluna, basta colocar elas neste formato (qualidade, estado)

-- COMMAND ----------

-- Removendo a chave do clustering 
ALTER TABLE logistica.vendas_pneus_cluster
CLUSTER BY NONE

-- COMMAND ----------

-- O comando abaixo funciona igual o DESCRIBE EXTENDED 
DESCRIBE FORMATTED logistica.vendas_pneus_cluster

-- COMMAND ----------

-- Testando se realmendo não dá para fazer tabelas particionadas com clustering 
create or replace table logistica.vendas_pneus_teste (
  id INT,
  modelo STRING,
  data_producao DATE,
  quantidade INT,
  qualidade STRING,
  estado STRING
)
PARTITIONED BY (estado)
CLUSTER BY (qualidade)

-- Realmente não funciona 

-- COMMAND ----------

-- O comando optimize força o reclustering da tabela
OPTIMIZE logistica.vendas_pneus_cluster

-- COMMAND ----------

describe history logistica.vendas_pneus_cluster; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE AS SELECT (CTAS) -- CREATE TABLE [USING]
-- MAGIC
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using#as

-- COMMAND ----------

-- Criando CTA com CLUSTER BY 
create or replace table logistica.cta_vendas_pneus as 
select * from logistica.vendas_pneus_cluster 
cluster by (qualidade) 

-- COMMAND ----------

select * from logistica.cta_vendas_pneus

-- COMMAND ----------

INSERT INTO logistica.vendas_pneus_cluster (id, modelo, data_producao, quantidade, qualidade, estado) VALUES
(6, 'Modelo A', '2024-06-01', 310, 'Alta', 'MG')

-- COMMAND ----------

describe history logistica.vendas_pneus_cluster

-- COMMAND ----------

-- Criando CTA usando o PARTITIONED BY e adicionando informações 
create or replace table logistica.cta_configuracoes_vendas_pneus

COMMENT   'Cta de teste venda pneus'
PARTITIONED BY (UF)
-- LOCATION '/mnt/dados/logistica/cta_configuracoes_vendas_pneus'

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

show partitions logistica.cta_configuracoes_vendas_pneus

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE IF NOT EXISTS e CREATE OR REPLACE TABLE não são a mesma coisa.
-- MAGIC
-- MAGIC - **CREATE TABLE IF NOT EXISTS** cria a tabela apenas se ela não existir. Se a tabela já existir, o comando não faz nada.
-- MAGIC - **CREATE OR REPLACE TABLE** cria a tabela, substituindo qualquer tabela existente com o mesmo nome. Isso significa que a tabela existente será descartada e uma nova tabela será criada. (Funciona tipo Overwrite)

-- COMMAND ----------

SHOW TABLES;


-- COMMAND ----------

DROP SCHEMA logistica CASCADE; 
