# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 800px">
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

# MAGIC %scala
# MAGIC 
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC 
# MAGIC var tmpFile = new File("/tmp/nfe.xml")
# MAGIC 
# MAGIC FileUtils.copyURLToFile(new URL("https://raw.githubusercontent.com/Databricks-BR/tax_lakehouse/main/datasets/NFe_XML/NFe_001.xml"), tmpFile)

# COMMAND ----------

# MAGIC 
# MAGIC %python
# MAGIC import re
# MAGIC 
# MAGIC filename = "nfe.xml"
# MAGIC username = spark.sql("SELECT current_user()").first()[0]
# MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
# MAGIC working_dir = f"dbfs:/user/{clean_username}/db_tax"
# MAGIC db_name = f"db_br_tax"
# MAGIC dbutils.fs.cp('file:/tmp/nfe.xml', working_dir + '/{filename}')
# MAGIC nfe_path = f"dbfs:/user/{clean_username}/db_tax/{filename}"
# MAGIC 
# MAGIC print("NFe XML Path: " + nfe_path)

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
