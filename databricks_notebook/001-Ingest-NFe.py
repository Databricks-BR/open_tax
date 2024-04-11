# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 700px"><a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_github.png" style="width: 40px; height: 40px;"></a>
# MAGIC    <a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks-br.png" style="width: 40px; height: 40px;"></a>  <a href="https://www.linkedin.com/groups/14100135"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_linkedin.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.meetup.com/pt-BR/databricks-brasil-oficial"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_meetup.png" style="height: 40px;"></a>  <a href="https://bit.ly/databricks-slack-br"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_slack.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.youtube.com/channel/UCH3cq9mit-0UkTu1mTki20Q"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_youtube.png" style="height: 38px;"></a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão Nota Fiscal Eletrônica - NFe - XML
# MAGIC </br>
# MAGIC
# MAGIC ##### Referências:
# MAGIC * https://docs.databricks.com/data/data-sources/xml.html
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/data/data-sources/xml
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/data/data-sources/xml#xsd-support

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO DO
# MAGIC
# MAGIC * Fazer a leitura da estrutura e comentários dos Campos - arquivo XSD
# MAGIC * Ver como fazer a leitura de vários XML (Autoloader), garantindo a integridade referencial do num das NFe
# MAGIC * Fazer o EXPLODE do XML aninhado

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC
# MAGIC var tmpFile = new File("/tmp/tax_lakehouse/nfe.xml")
# MAGIC
# MAGIC FileUtils.copyURLToFile(new URL("https://raw.githubusercontent.com/Databricks-BR/tax_lakehouse/main/datasets/NFe_XML/NFe_001.xml"), tmpFile)

# COMMAND ----------

import re

filename = "nfe.xml"
username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
working_dir = f"dbfs:/user/{clean_username}/db_tax"
db_name = f"db_br_tax"
dbutils.fs.cp('file:/tmp/nfe.xml', working_dir + '/{filename}')
nfe_path = f"dbfs:/user/{clean_username}/db_tax/{filename}"

print("NFe XML Path: " + nfe_path)

# COMMAND ----------

dbutils.fs.head(nfe_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.xml._ 
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC import spark.implicits._

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read
# MAGIC   .option("rowTag", "dest")
# MAGIC   .xml("dbfs:/user/luis_assuncao_databricks_com/db_tax/nfe.xml")
# MAGIC
# MAGIC display(df)
