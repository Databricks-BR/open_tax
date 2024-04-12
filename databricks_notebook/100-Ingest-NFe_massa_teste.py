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
# MAGIC | **Objetivo Pipeline** | Carrega uma **MASSA DE TESTE - Somente pra DEV** |
# MAGIC | **Camada** | Carga pra LANDING |
# MAGIC | **Databricks Run Time** | DBR 14.3 LTS |
# MAGIC | **Linguagem** | Python, Pyspark |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 11-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carrega uma MASSA de TESTE de Notas Fiscais
# MAGIC ##### Em produção, ignorar esse PIPELINE

# COMMAND ----------


for i in range(1,8):

  git_url = "https://raw.githubusercontent.com/Databricks-BR/open_tax/main/datasets/NFe_XML/NFe_00" + str(i) + ".xml"
  dbfs_path = "/Volumes/tax/landing/nfe_xml/NFe_00" + str(i) + ".xml"
  response = requests.get(git_url)

  with open(dbfs_path, "wb") as f:
    f.write(response.content)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida o Conteúdo do Arquivo

# COMMAND ----------

dbutils.fs.head("dbfs:/Volumes/tax/landing/nfe_xml/NFe_002.xml")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valida a estrutura do XML. (Schema com suas hierarquias)

# COMMAND ----------


xmlPath = "dbfs:/Volumes/tax/landing/nfe_xml/NFe_001.xml"

df = spark.read.option("rowTag", "infNFe").format("xml").load(xmlPath)
df.printSchema()


# COMMAND ----------

xmlPath = "dbfs:/Volumes/tax/landing/nfe_xml/NFe_001.xml"

df_prod = spark.read.option("rowTag", "prod").format("xml").load(xmlPath)
df_prod.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualisa conteúdo do DataFrame

# COMMAND ----------

display(df)


# COMMAND ----------

display(df_prod)

