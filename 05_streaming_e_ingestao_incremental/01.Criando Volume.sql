
-- Volumes gerenciados
create volume  if not exists infinitybd.default.teste2;
drop volume infinitybd.default.teste2;


-- Schemas e volumes externos
CREATE SCHEMA IF NOT EXISTS infinitybd.azure;
create external volume infinitybd.azure.projet1 -- volume aparecer no catalog Databricks 
comment 'Volume externo na Azure'
location 'abfss://uc-ext-azure@externalazure.dfs.core.windows.net/projet1' -- pasta dentro do Azure  ideal Volume e Catalog e pasta azure ter o mesmo nome


-- Schemas e volumes externos
create external volume infinitybd.azure.logistica -- volume aparecer no catalog Databricks 
comment 'Volume externo na Azure  para logistica '
location 'abfss://uc-ext-azure@externalazure.dfs.core.windows.net/logistica' -- pasta dentro do Azure  ideal Volume e Catalog e pasta azure ter o mesmo nome


-- Drop teste para testar o acesso e segurança externa 
drop volume infinitybd.azure.`projet1`;
drop volume infinitybd.azure.`logistica`;
drop schema infinitybd.azure;

-- montar novamente , terei os mesmos dados das pastas?

