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
# MAGIC | **Camada** | Bronze to Silver |
# MAGIC | **Databricks Run Time** | DBR 14.3 LTS |
# MAGIC | **Linguagem** | SQL |
# MAGIC | **Framework de Carga** | [AUTOLOADER - spark.stream](https://docs.databricks.com/en/ingestion/auto-loader/patterns.html#load-csv-files-without-headers) |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 16-ABR-2024 | Wagner Santos | wagner.santos@databricks.com | Primeira versão  |

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/etl_sped_efd.png" width="900px">

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.sped_efd 
# MAGIC as
# MAGIC select rowtype
# MAGIC ,case when (size(label_array) == size(values_array)) then to_json(map_from_arrays(label_array, values_array)) else null end as json
# MAGIC ,filename, dat_process, time_process
# MAGIC , values_array, label_array, value
# MAGIC --,size(label_array) as s_lab, size(values_array) as s_val
# MAGIC from 
# MAGIC (
# MAGIC   SELECT * FROM
# MAGIC     (SELECT substr(value,2,4) as rowtype, split(substr(replace(value, '|','¬'),2,length(value) - 2),'¬') as values_array, *
# MAGIC     FROM tax.bronze.sped_efd_contrib)
# MAGIC     JOIN (
# MAGIC         SELECT bloco, split(concat_ws('¬', collect_list(campo)),'¬') as label_array
# MAGIC         FROM tax.metadado.layout_sped_efd_blocos
# MAGIC         GROUP BY bloco
# MAGIC     ) A 
# MAGIC     ON rowtype = bloco
# MAGIC )
# MAGIC
