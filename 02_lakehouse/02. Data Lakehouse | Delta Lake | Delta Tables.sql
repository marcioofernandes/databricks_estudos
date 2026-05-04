-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Data Lakehouse 
-- MAGIC
-- MAGIC **Data lakehouse** é uma junção do **data lake** com o **data warehouse**, ou seja, ele une os atributos do data lake como a flexibilidade nos formatos dos arquivos permitidos, a economia e a escalabilidade do data lake com o gerenciamento dos dados estruturados e os recursos **_ACID_** do data warehouse. Isso permite que armazene dados estruturados e não estruturados no mesmo ambiente e acelera o processamento de dados. 
-- MAGIC
-- MAGIC ![lakehouse](/Workspace/Users/marc.fernandes13@gmail.com/databricks_estudos/images/lakehouse.jpg)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Lake 
-- MAGIC
-- MAGIC **Delta lake** é uma camada de armazenamento de código aberto (open source). Ele foi projetado para suportar transações ACID dentro do Delta Lake, além disso oferece manipulação escalável dos metadados e promove o processamento em batch ou streaming na mesma plataforma.
-- MAGIC
-- MAGIC ### Transações ACID
-- MAGIC
-- MAGIC - **Atomicidade:** Significa que ou a transação é bem sucedida ou ela falha completamente. 
-- MAGIC - **Consistência:** É a forma como os dados são apresentados mesmo durante operações. simultaneas, ou seja, você pode consultar uma tabela mesmo durante uma operação de write de novos dados. 
-- MAGIC - **Isolamento:** um pouco do que foi dito acima, as operações não conflitam entre si. 
-- MAGIC - **Durabilidade:** Significa que as alterações realizadas são permanentes. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Tables
-- MAGIC
-- MAGIC É o formato padrão de tabelas do **Databricks**, elas utilizam o framework do Delta Lake. Uma tabela Delta armazena fisicamente os dados em um diretório de arquivos de armazenamento (ex: dbfs, blob, etc) assim como seus logs. Isso quer dizer que sempre que você cria uma nova tabela, ela vai existir fisicamente em algum path (_ex: dbfs:workspace/sua_tabela_) e dentro desse path vai conter arquivos **parquet** com os metadados da tabela e arquivo **json** que registram os logs de alterações da tabela (ACID)
-- MAGIC
-- MAGIC **Matadados e Schemas** : As Deltas tables armazem a estrutura dos dados (schema da tabela), o particionamento e as configurações diretamente nos logs. ele obriga que os dados gravados obedeçam a estrutura predefinida, prevenindo que dados corrompidos entre nas tabelas 
-- MAGIC
-- MAGIC #### Recursos e Diferenciais: 
-- MAGIC
-- MAGIC - **Transações ACID e DML**: permite o uso de SQL padrão, como INSERT, UPDATE, DELETE, MERGE para trabalhar com dados de forma simples. **obs** Só realiza transações de uma tabela por vez. 
-- MAGIC
-- MAGIC - **Une Batch e Streaming**: Permite atuar simultaneamente como fonte e destino (sinks) para carga de trabalho streaming e bath resolvendo problemas como fusão de arquivos pequenos vindos do streaming e o processamento pesado do **exactly-once** (pesquisar sobre isso)
-- MAGIC
-- MAGIC - **Time-travel**: É possivel voltar alterações realizadas graças aos logs que gravam snapshots de versões anteriores da tabela. 
-- MAGIC
-- MAGIC - **Schema evolution**: Permite adicionar colunas em tabelas já prontas sem quebrar nada.  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos criar **Delta Tables** na prática, no primeiro momento do notebook usaremos apenas **comandos SQL** e na segunda parte faremos a equivalencia usando **PySpark**

-- COMMAND ----------

-- Criando Delta Tables, método mais simples possível 

CREATE TABLE vendas(
  Id INT, 
  nome_cliente STRING, 
  data_da_compra TIMESTAMP, 
  valor_compra DOUBLE
); 

-- Quando não passamos o caminho completo catalog.schema.table, ela é criada no workspace default

-- COMMAND ----------

-- Garante que a tabela só será criada caso ela não exista

CREATE TABLE IF NOT EXISTS vendas(
  Id INT, 
  nome_cliente STRING, 
  data_da_compra TIMESTAMP, 
  valor_compra DOUBLE
);

-- COMMAND ----------

-- Util para criar ou atualizar uma tabela já existente (ela funciona como um overwrite da tabela já criada) 

CREATE OR REPLACE TABLE vendas(
  Id INT, 
  nome_cliente STRING, 
  data_da_compra TIMESTAMP, 
  valor_compra DOUBLE
);

-- COMMAND ----------

-- O comando DESCRIBE TABLE permite visualizar o schema da tabela, como nome de colunas, data types e comentários
DESCRIBE TABLE vendas;

-- COMMAND ----------

-- Para inserir dados devemos respeitar o esquema da tabela passando os valores na ordem correta das colunas 

INSERT INTO vendas (Id, nome_cliente, data_da_compra, valor_compra) 
VALUES 
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

--DESCRIBE DETAIL te dá mais informações sobre os detalhes da tabela como por exemplo seu formato, location, data de criação, caminho criado, etc 
DESCRIBE DETAIL workspace.default.vendas

-- O campo 'location' do resultado mostra o caminho no storage onde estão os arquivos de log (_delta_log) e os dados da tabela. Nota que neste caso a coluna "location" está vazia. 

-- Houve mudanças com o UC, agora o caminho onde os arquivos de logs e arquivos parquet da tabela estão armazenados no storage do UC, não podendo mais ser acessado, portando comandos como ls _delta_log não funcionam mais.


-- COMMAND ----------

-- MAGIC %md
-- MAGIC A maneira correta de verificar históricos de log em tabelas UC managed é usando o comando describe, abaixo vou fazer a adaptação dos comandos utilizados para maneira que funciona no UC, porém é importante lembrar que esses comandos "antigos" ainda funcionam, só depende de como a tabela foi feita. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O comando abaixo funciona para external tables, ou para tabelas criadas dentro de volumes do UC

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/vendas/_delta_log/''

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head 'dbfs:/user/hive/warehouse/vendas/_delta_log/00000000000000000003.json'

-- COMMAND ----------


-- Mesmo comando acima para tabelas gerenciadas pelo UC
DESCRIBE HISTORY vendas;

-- Veja que interessante, ao rodar o comando CREATE TABLE geramos a versão 1 da tabela, porém  ao rodar o comando CREATE OR REPLACE TABLE, não apagamos o histórico antigo, gerando uma versão 2 da tabela, com isso seria possivel fazer o time travel aqui. 


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
-- MAGIC
-- MAGIC Novamente, só temos acesso a esses arquivos em versões mais antigas do Databricks ou caso suas tabelas não sejam gerenciadas pelo Unity Catalog. 

-- COMMAND ----------

-- Comando para fazer update em dados da tabela 
UPDATE vendas 
SET valor_compra = 499.99
where Id = 10; 

-- Caso queria fazer mais de uma alteração no mesmo comando SET, abaixo alguns exemplos 

-- COMMAND ----------

-- Múltiplas colunas no mesmo SET 

UPDATE vendas 
SET valor_compra = 123.45,
    nome_cliente = "Bruce Wayne"
WHERE Id = 5; 

-- COMMAND ----------

-- Múltiplas linhas com condições diferentes 

UPDATE vendas 
SET valor_compra = CASE 
  WHEN nome_cliente = "Paulo" THEN 350.00
  WHEN nome_cliente = "Ana" THEN 155.00
  ELSE valor_compra
END
WHERE nome_cliente IN ('Paulo', 'Ana');

-- SEMPRE passe o ELSE no final, caso esqueça, o código vai aplicar NULL para todos os registros que não estão dentro da condicional 
-- END encerra a condicional, sem ela, dá sintax error 
-- Por que o WHERE IN no final? Sem o WHERE IN no final o código também iria funcionar porém com menos performance, sem ele o código iria percorrer todas as linhas da tabela e para cada linha que não esta na condicional ele sobrescrever com o valor da condicional, isso pode gerar um overhead desnecessário. (Nota que ao rodar o código sem o WHERE aparece 10 linhas foram modificadas, por que todas que não entravam na condicional foram sobreescritas)
-- Já quando a gente passa o WHERE IN ele só vai processar as linhas necessárias (Rodei novamente com o where in, e somente 2 linhas foram processadas)



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
-- MAGIC Como ver o histórico de transações dentro do Unity Catalog?? 
-- MAGIC

-- COMMAND ----------

--DESCRIBE HISTORY - ver Histórico de alterações
DESCRIBE HISTORY vendas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Equivalências em PySpark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Existe duas formas de criar tabelas com Pyspark, mas primeiro você precisa entender o conceito, o pyspark funciona através de dataframes, ou seja, primeiro você cria um dataframe, e depois a tabela a partir dele. Abaixo há duas formas de fazer isso: 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Maneira simples, com query de SQL, neste caso não há necessidade de fazer o df antes, pois o próprio comando, nada mais é que o comando de criar tabela em SQL, basicamente o que fez, foi adicionar um spark.sql na frente. 
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC     CREATE OR REPLACE TABLE vendas_2 (
-- MAGIC         Id INT,
-- MAGIC         nome_cliente STRING,
-- MAGIC         data_da_compra TIMESTAMP,
-- MAGIC         valor_compra DOUBLE
-- MAGIC     )
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Maneira mais complexa, usando o pyspark, onde você cria um dataframe vazio com o schema desejado. 
-- MAGIC
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
-- MAGIC
-- MAGIC # Definir o schema
-- MAGIC schema = StructType([
-- MAGIC     StructField("Id", IntegerType(), True),
-- MAGIC     StructField("nome_cliente", StringType(), True),
-- MAGIC     StructField("data_da_compra", TimestampType(), True),
-- MAGIC     StructField("valor_compra", DoubleType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Criar DataFrame vazio com esse schema
-- MAGIC df = spark.createDataFrame([], schema=schema)
-- MAGIC
-- MAGIC # Salvar como tabela
-- MAGIC df.write.mode("overwrite").saveAsTable("vendas_3")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC E agora você deve estar se perguntando? Se o spark.sql nada mais é que queries de SQL dentro do spark, por que não usar SQL direto? 
-- MAGIC
-- MAGIC E aqui está a grande vantagem de utilizar o spark.sql no desenvolvimento, é sua simplicidade e integração com código python, já que por mais que estamos trabalhando com SQL dentro do Spark, na verdade, ainda assim, estamos trabalhando com DATAFRAMES
-- MAGIC
-- MAGIC E isso permite explorar o código tornando o muito mais flexivel, além de 
-- MAGIC
-- MAGIC SQL puro → Resultado é apenas um texto ou saída no console
-- MAGIC spark.sql() → Resultado é um DataFrame reutilizável que você pode manipular, salvar, transformar, etc.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Consultando com spark.sql 
-- MAGIC df = spark.sql("SELECT * FROM workspace.default.vendas")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Consultando com spark puro 
-- MAGIC
-- MAGIC df = spark.read.table("vendas_2")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Salvar o dataframe como tabela 
-- MAGIC df.write.mode("overwrite").saveAsTable("vendas_2")
