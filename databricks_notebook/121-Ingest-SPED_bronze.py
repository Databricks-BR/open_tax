# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/header_opentax.png" width="800px">
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | Item | Descrição |
# MAGIC | --- | --- |
# MAGIC | **Objetivo Pipeline** | Ingestão SPED EFD Contribuições |
# MAGIC | **Camada** | Landing to Bronze |
# MAGIC | **Databricks Run Time** | DBR 14.3 LTS |
# MAGIC | **Linguagem** | Python, Pyspark |
# MAGIC | **Referência** | [Leitura de formatos XML](https://docs.databricks.com/en/query/formats/xml.html) |
# MAGIC | **Framework de Carga** | [AUTOLOADER - spark.stream pra XML](https://docs.databricks.com/en/query/formats/xml.html#read-and-write-xml) |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 10-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/etl_sped_efd.png" width="900px">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parâmetros
# MAGIC | Parâmetro | descrição | 
# MAGIC | --- | --- | 
# MAGIC | rootpath | diretório raiz onde serão armazenados os arquivos de schema e checkpoint |
# MAGIC | inputPath | path do LANDING onde estão os arquivos XML (dbfs:/Volumes...) |
# MAGIC | schemaPath | path+arquivo pra controle de atualização do layout do XML |
# MAGIC | checkPointPath | diretório de controle pra marcação dos arquivos já processados |
# MAGIC | delta_table | tabela DELTA de destino na camada BRONZE | 

# COMMAND ----------

# DBTITLE 1,ATRIBUIÇÃO DOS PARÂMETROS
rootpath = "/tmp/tax/landing/sped_efd/"
inputPath = "dbfs:/Volumes/tax/landing/sped/"
schemaPath =     rootpath + "schema/sped_schema.json"
checkPointPath = rootpath + "_checkpoints/"
delta_table = "tax.bronze.sped_efd_contrib"


# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DO DIRETORIO DE CONTROLE (Checkpoint)
# Delete the existing checkpoint directory 
# dbutils.fs.rm(checkPointPath, True)

# Create a new checkpoint directory
dbutils.fs.mkdirs(checkPointPath)

# COMMAND ----------

# DBTITLE 1,AUTOLOADER
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import col


query = (spark
  .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")                     # formato do arquivo TEXTO
    .option("cloudFiles.schemaLocation", schemaPath )        # local onde está o schema do TEXTO (controle de mudança)
    .load(inputPath)                                         # caminho do diretório de entrada (Landing Zone)
    .withColumn("filename", col("_metadata.file_path"))      # captura o nome do arquivo fisico
    .withColumn("dat_process", current_date())               # captura a data de processamento
    .withColumn("time_process", current_timestamp())         # captura o horário de processamento
    .writeStream
    .format("delta")
    .outputMode("append")                                   # opção de adicionar ou sobrescrever os dados
    .option("mergeSchema", "true")                          # mergeSchema = true, para evitar falhas de schema
    .option("checkpointLocation", checkPointPath)           # controle dos arquivos TEXTO já processados
    .trigger(once=True)                                     # Modo Batch
    .toTable(delta_table)                                   # Nome da tabela delta destino
    )

# Wait for the streaming query to finish
query.awaitTermination()


