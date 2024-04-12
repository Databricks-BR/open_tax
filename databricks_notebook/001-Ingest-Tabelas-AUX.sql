-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/header_opentax.png" width="800px">
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Controle de Versão do Código (Pipeline)
-- MAGIC
-- MAGIC | versão | data | autor | e-mail | alterações |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 1.0 | 12-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Descrição e Objetivos desse Pipeline
-- MAGIC
-- MAGIC | projeto | aplicação | módulo | tabela | objetivo |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | Open TAX | Tax Lakehouse | Carga ETL | Tabelas Auxiliares | Ingestão inicial - ONE TIME |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cargas de TABELAS fixas
-- MAGIC #### Tabelas Auxiliares de Tipificação (Códigos + Descrição)
-- MAGIC
-- MAGIC | # | Fonte | Descrição |
-- MAGIC | -- | -- | -- |
-- MAGIC | 01 | CNAE	| Classificação Nacional de Atividades Econômicas |
-- MAGIC | 02 | CFOP	| Código Fiscal de Operações e Prestações  |
-- MAGIC | 03 | NCM	| Nomenclatura Comum do Mercosul  |
-- MAGIC | 04 | SIT NFe | Código de Situação da Nota Fiscal |
-- MAGIC | 05 | CST PIS | Código de Situação Tributária do PIS |
-- MAGIC | 06 | CST IPI| Código de Situação Tributária do IPI |
-- MAGIC | 07 | CST COFINS | Código de Situação Tributária do COFINS |
-- MAGIC
-- MAGIC  

-- COMMAND ----------

-- DBTITLE 1,LINK COM O REPOSITÓRIO GITHUB DO PROJETO OPEN TAX
-- MAGIC %python
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC url = f"https://raw.githubusercontent.com//Databricks-BR/open_tax/main/lakehouse/"
-- MAGIC catalog_name = f"tax"
-- MAGIC schema_name = f"silver"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 01) Tabela CNAE - Classificação Nacional de Atividades Econômicas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC entity_name  = f"tab_cnae"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta     

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_cnae IS 'CNAE significa Classificação Nacional de Atividades Econômicas. Trata-se de um código utilizado para identificar quais são as atividades econômicas exercidas por uma empresa.';

ALTER TABLE tab_cnae ALTER COLUMN cod_cnae COMMENT 'Código da Classificação Nacional de Atividades Econômicas';
ALTER TABLE tab_cnae ALTER COLUMN desc_cnae COMMENT 'Descrição da atividades econômicas exercidas por uma empresa';
-- ALTER TABLE tab_cnae ADD CONSTRAINT tab_cnae_pk PRIMARY KEY(cod_cnae);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02) Tabela CFOP - Código Fiscal de Operações e de Prestações

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC entity_name  = f"tab_cfop"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite") \
-- MAGIC     .option("overwriteSchema", "true") \
-- MAGIC     .saveAsTable(table_name)                         # grava o DataFrame na Tabela Delta  

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_cfop IS 'Código Fiscal de Operações e de Prestações das Entradas de Mercadorias e Bens e da Aquisição de Serviços ou sob a sigla CFOP é um código do sistema tributarista brasileiro, determinado pelo governo.  É indicado nas emissões de notas fiscais, declarações, guias e escrituração de livros. É utilizado em uma operação fiscal e define se a nota emitida recolhe ou não impostos, movimento de estoque e financeiro.';

ALTER TABLE tab_cfop ALTER COLUMN cod_cfop COMMENT 'Código Fiscal de Operações e Prestações';
ALTER TABLE tab_cfop ALTER COLUMN desc_cfop COMMENT 'Descrição do Código Fiscal de Operações e Prestações';
-- ALTER TABLE tab_cfop ADD CONSTRAINT tab_cfop_pk PRIMARY KEY(cod_cfop);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03) Tabela NCM - Nomenclatura Comum do Mercosul

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC entity_name  = f"tab_ncm"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta  

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_ncm IS 'NCM significa Nomenclatura Comum do Mercosul e trata-se de um código de oito dígitos estabelecido pelo Governo Brasileiro para identificar a natureza das mercadorias e promover o desenvolvimento do comércio internacional, além de facilitar a coleta e análise das estatísticas do comércio exterior. Qualquer mercadoria, importada ou comprada no Brasil, deve ter um código NCM na sua documentação legal (nota fiscal, livros legais, etc.), cujo objetivo é classificar os itens de acordo com regulamentos do Mercosul.';

ALTER TABLE tab_ncm ALTER COLUMN cod_ncm COMMENT 'Código da Mercadoria';
ALTER TABLE tab_ncm ALTER COLUMN nom_categoria_ncm COMMENT 'Nome da Categoria do Produto';
ALTER TABLE tab_ncm ALTER COLUMN desc_ncm COMMENT 'Código e Descrição da Mercadoria';
ALTER TABLE tab_ncm ALTER COLUMN nom_ncm COMMENT 'Descrição da Mercadoria';
-- ALTER TABLE tab_ncm ADD CONSTRAINT tab_tab_ncm PRIMARY KEY(cod_ncm);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 04) Tabela SIT NFe - Código de Situação da Nota Fiscal 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC entity_name  = f"tab_situacao_nf"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta  

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_situacao_nf IS 'Tabela de situação da Nota Fiscal eletrônica';

ALTER TABLE tab_situacao_nf ALTER COLUMN cod_situacao_nf COMMENT 'Código da situação da Nota Fiscal';
ALTER TABLE tab_situacao_nf ALTER COLUMN desc_situacao_nf COMMENT 'Descrição da situação da Nota Fiscal';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 05) Tabela CST PIS - Código de Situação Tributária do PIS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC entity_name  = f"tab_cst_pis"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta 

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_cst_pis IS 'A tabela do CST_IPI (Código da Situação Tributária do IPI) consta publicada na Instrução Normativa RFB nº 932, de 14/04/2009. A partir de 01 de abril de 2010, IN RFB nº 1009, de 10 de fevereiro de 2010.';

ALTER TABLE tab_cst_pis ALTER COLUMN cod_cst_pis COMMENT 'Código da situação tributária do PIS';
ALTER TABLE tab_cst_pis ALTER COLUMN desc_cst_pis COMMENT 'Descrição da situação tributária do PIS';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 06) Tabela CST IPI - Código de Situação Tributária do IPI 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC entity_name  = f"tab_cst_ipi"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta 

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_cst_ipi IS 'CST_IPI (Código da Situação Tributária do IPI)';

ALTER TABLE tab_cst_ipi ALTER COLUMN cod_cst_ipi COMMENT 'Código da situação tributária do IPI';
ALTER TABLE tab_cst_ipi ALTER COLUMN desc_cst_ipi COMMENT 'Descrição da situação tributária do IPI';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 07) Tabela CST COFINS - Código de Situação Tributária do COFINS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC entity_name  = f"tab_cst_cofins"
-- MAGIC table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
-- MAGIC file_name = f"{url}{entity_name}.csv"
-- MAGIC
-- MAGIC df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
-- MAGIC s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
-- MAGIC s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta 

-- COMMAND ----------

USE catalog tax;
USE silver;
COMMENT ON TABLE tab_cst_cofins IS 'Tabela CST_COFINS significa Código da Situação Tributária referente ao COFINS, constante da Instrução Normativa RFB nº 932, de 14/04/2009.';

ALTER TABLE tab_cst_cofins ALTER COLUMN cod_cst_cofins COMMENT 'Código da situação tributária do COFINS';
ALTER TABLE tab_cst_cofins ALTER COLUMN desc_cst_cofins COMMENT 'Descrição da situação tributária do COFINS';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RASCUNHO
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,VALIDAÇÃO

SELECT table_catalog, table_schema, table_name, comment
FROM
  tax.information_schema.tables
WHERE 
  table_type = 'MANAGED'
