# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestão Nota Fiscal Eletronica - NFe - XML
# MAGIC <br>
# MAGIC ### Referências:<br>
# MAGIC 
# MAGIC * https://docs.databricks.com/data/data-sources/xml.html

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC 
# MAGIC var tmpFile = new File("/tmp/nfe.xml")
# MAGIC FileUtils.copyURLToFile(new URL("https://raw.githubusercontent.com/lcpassuncao/databricks_br_tax/main/test_nfe_xml/NFe35170171322150001301550000000477521876550573_procNFe.xml"), tmpFile)

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
