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
# MAGIC | **Objetivo Pipeline** | Carrega uma **MASSA DE TESTE (SPED) - Somente pra DEV** |
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
# MAGIC | 1.0 | 12-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carrega uma MASSA de TESTE de arquivos SPED (EFD Contribuições)
# MAGIC ##### Em produção, ignorar esse PIPELINE

# COMMAND ----------


for i in range(1,5):

  
  git_url = "https://raw.githubusercontent.com/Databricks-BR/open_tax/main/datasets/EFD_Contrib/efd_contrib_0" + str(i) + ".txt"
  dbfs_path = "/Volumes/tax/landing/sped/efd_contrib_0" + str(i) + ".txt"
  response = requests.get(git_url)

  with open(dbfs_path, "wb") as f:
    f.write(response.content)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valida o Conteúdo do Arquivo

# COMMAND ----------

dbutils.fs.head("dbfs:/Volumes/tax/landing/sped/efd_contrib_02.txt")
