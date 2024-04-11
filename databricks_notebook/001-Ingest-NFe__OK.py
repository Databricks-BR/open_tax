# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/header_opentax.png" width="800px">
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão Nota Fiscal Eletrônica - NFe - XML
# MAGIC </br>
# MAGIC
# MAGIC #### Databricks Run Time (DRM):  14.3 LTS
# MAGIC
# MAGIC ##### Referências:
# MAGIC * https://docs.databricks.com/en/query/formats/xml.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 10-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO DO
# MAGIC
# MAGIC * Fazer a leitura da estrutura e comentários dos Campos - arquivo XSD
# MAGIC * Ver como fazer a leitura de vários XML (Autoloader), garantindo a integridade referencial do num das NFe
# MAGIC * Fazer o EXPLODE do XML aninhado

# COMMAND ----------

dbutils.fs.head("dbfs:/Volumes/luis_assuncao/tax_lakehouse/dbtax/NFe_001.xml")
#dbutils.fs.head("s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/volumes/1ff7d7aa-93de-4b87-a1f2-9be01e8b4c38/NFe_001.xml")


# COMMAND ----------

# Cria um subdiretorio temporario para cópia
dbutils.fs.mkdirs("/tmp/tax_lakehouse")

# inicializacao de variaveis (constantes)
# localizacao dos arquivos de entrada e saida
working_dir = "dbfs:/Volumes/luis_assuncao/tax_lakehouse/dbtax/"
tmpPath = "/tmp/tax_lakehouse/"
tmpFileXML = tmpPath + "nfe.xml"
dbfsFileXML = working_dir + "NFe_001.xml"

# faz a copia para o DBFS (Databricks File System)
dbutils.fs.cp( dbfsFileXML, "file://" + tmpFileXML )

dbutils.fs.head("file://" + tmpFileXML)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOVA LIB
# MAGIC https://docs.databricks.com/en/query/formats/xml.html

# COMMAND ----------


xmlPath = "dbfs:/Volumes/luis_assuncao/tax_lakehouse/dbtax/NFe_001.xml"

df = spark.read.option("rowTag", "infNFe").format("xml").load(xmlPath)
df.printSchema()

df_nfe = df.select("_id")

#df.show(truncate=False)
display(df)

display(df_nfe)

# COMMAND ----------

xmlPath = "dbfs:/Volumes/luis_assuncao/tax_lakehouse/dbtax/NFe_001.xml"

df_prod = spark.read.option("rowTag", "prod").format("xml").load(xmlPath)

df_prod.printSchema()

display(df_prod)

# COMMAND ----------

df_nfe_prod = df_nfe.join(df_prod)

display(df_nfe_prod)


# COMMAND ----------

df_nfe_prod.write.mode("overwrite").saveAsTable("tax.bronze.nfe_prod")

# COMMAND ----------

df_ICMSTot = spark.read.option("rowTag", "ICMSTot").format("xml").load(xmlPath)

df_ICMSTot.printSchema()

display(df_ICMSTot)

# COMMAND ----------

df_nfe_ICMSTot = df_nfe.join(df_ICMSTot)
df_nfe_ICMSTot.write.mode("overwrite").saveAsTable("tax.bronze.nfe_ICMSTot")


# COMMAND ----------

from pyspark.sql.functions import explode

df_emit = spark.read.option("rowTag", "emit").format("xml").load(xmlPath)
df_emit = df_emit.drop("enderEmit")

df_enderEmit = spark.read.option("rowTag", "enderEmit").format("xml").load(xmlPath)
df_emit_enderEmit = df_emit.join(df_enderEmit)
df_emit_enderEmit = df_nfe.join(df_emit_enderEmit)

df_emit_enderEmit.printSchema()


display(df_emit_enderEmit)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTO LOADER
# MAGIC
# MAGIC
# MAGIC * https://docs.databricks.com/en/query/formats/xml.html#read-and-write-xml

# COMMAND ----------


query = (spark
  .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "xml")
    .option("rowTag", "infNFe")
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", schemaPath)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(inputPath)
    .writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkPointPath)
    .trigger(Trigger.AvailableNow())
    .toTable("tax.bronze.nfe")
    )

query = query.start(outputPath).awaitTermination()
df = spark.read.format("delta").load(outputPath)
df.show()

# COMMAND ----------

(events.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
   .toTable("events")
)
