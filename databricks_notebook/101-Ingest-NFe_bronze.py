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
# MAGIC | **Objetivo Pipeline** | Ingestão Nota Fiscal Eletrônica - NFe - XML |
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
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/etl_nfe.png" width="900px">
# MAGIC

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
rootpath = "/tmp/tax/landing/nfe_xml/"
inputPath = "dbfs:/Volumes/tax/landing/nfe_xml/"
schemaPath =     rootpath + "schema/nfe_xml_schema.json"
checkPointPath = rootpath + "_checkpoints/"
delta_table = "tax.bronze.nfe_xml"


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
    .option("cloudFiles.format", "xml")                     # formato do arquivo XML
    .option("rowTag", "infNFe")                             # nome da tag raiz do XML
    .option("cloudFiles.inferColumnTypes", True)            # inferir os tipos das colunas (detecção do layout)
    .option("cloudFiles.schemaEvolutionMode", "rescue")     # verifica mudança no layout do XML, para evitar falhas
    .option("cloudFiles.schemaLocation", schemaPath)        # local onde está o schema do XML (controle de mudança)
    .load(inputPath)                                        # caminho do diretório de entrada (Landing Zone)
    .withColumn("filename", col("_metadata.file_path"))     # captura o nome do arquivo fisico
    .withColumn("dat_process", current_date())              # captura a data de processamento
    .withColumn("time_process", current_timestamp())        # captura o horário de processamento
    .writeStream
    .format("delta")
    .outputMode("append")                                   # opção de adicionar ou sobrescrever os dados
    .option("mergeSchema", "true")                          # mergeSchema = true, para evitar falhas de schema
    .option("checkpointLocation", checkPointPath)           # controle dos arquivos XML já processados
    .trigger(once=True)                                     # Modo Batch
    .toTable(delta_table)                                   # Nome da tabela delta destino
    )

# Wait for the streaming query to finish
query.awaitTermination()



# COMMAND ----------

# DBTITLE 1,Dicionariza a Tabela (executar apenas 1 vez)
# MAGIC %sql
# MAGIC
# MAGIC A tabela 'nfe_xml' contém informações sobre arquivos XML da NFe (Nota Fiscal Eletrônica). Inclui detalhes como versão, estrutura de vários componentes e metadados sobre o arquivo. Esses dados podem ser úteis para rastrear o processamento da NF-e, monitorar a qualidade dos arquivos e garantir a conformidade com as regulamentações. Ele também pode ajudar na identificação e resolução de problemas com arquivos XML da NFe, melhorando a eficiência geral e reduzindo erros no pipeline de processamento da NFe.
# MAGIC
