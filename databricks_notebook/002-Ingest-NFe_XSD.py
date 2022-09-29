# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 700px"><a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_github.png" style="width: 40px; height: 40px;"></a>
# MAGIC    <a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks-br.png" style="width: 40px; height: 40px;"></a>  <a href="https://www.linkedin.com/groups/14100135"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_linkedin.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.meetup.com/pt-BR/databricks-brasil-oficial"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_meetup.png" style="height: 40px;"></a>  <a href="https://bit.ly/databricks-slack-br"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_slack.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.youtube.com/channel/UCH3cq9mit-0UkTu1mTki20Q"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_youtube.png" style="height: 38px;"></a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão Nota Fiscal Eletrônica - NFe - XSD - Schema
# MAGIC </br>
# MAGIC 
# MAGIC ##### Referência técnica de leitura do XSD:
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/data/data-sources/xml#xsd-support

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Faz a copias dos arquivos a serem carregados (Origem:  Github.com/Databricks-BR)
# MAGIC 
# MAGIC // bibliotecas
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC 
# MAGIC // inicializacao de variaveis (constantes)
# MAGIC // localizacao dos arquivos de entrada e saida
# MAGIC var github = "https://raw.githubusercontent.com/Databricks-BR/tax_lakehouse/main/datasets/"
# MAGIC val working_dir = f"dbfs:/user/luis_assuncao/db_tax_lakehouse"
# MAGIC val db_name = f"db_tax_lakehouse"  // Database name
# MAGIC var tmpFileXML = new File("/tmp/nfe.xml")
# MAGIC var dbfsFileXML = new File(working_dir + "/nfe.xml")
# MAGIC var dbfsFileXSD = new File(working_dir + "/leiauteNFe_v4.00.xsd")
# MAGIC 
# MAGIC // faz a copia do GitHub de origem para um diretorio temporario 
# MAGIC FileUtils.copyURLToFile(new URL(github + "NFe_XML/NFe_001.xml"), tmpFileXML)
# MAGIC 
# MAGIC // faz a copia para o DBFS (Databricks File System)
# MAGIC FileUtils.copyFile(tmpFileXML, dbfsFileXML)
# MAGIC 
# MAGIC // faz a copia dos arquivos de estrutura do XML (schema)
# MAGIC var fileXSD = new Array[String](3)
# MAGIC fileXSD(0) = "leiauteNFe_v4.00.xsd"            // Layout principal da Nota Fiscal versao 4.00
# MAGIC fileXSD(1) = "xmldsig-core-schema_v1.01.xsd"   // Layout chamado (import) dentro do layout principal
# MAGIC fileXSD(2) = "tiposBasico_v4.00.xsd"           // Layout chamado (include) dentro do layout principal
# MAGIC 
# MAGIC for( iXSD <- 0 to 2){
# MAGIC       FileUtils.copyURLToFile(new URL(github + "XSD_schema/" + fileXSD(iXSD) ), new File("/tmp/" + fileXSD(iXSD) ));
# MAGIC       FileUtils.copyFile(  new File("/tmp/" + fileXSD(iXSD) ), new File(working_dir + fileXSD(iXSD) ) )      
# MAGIC    }

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // bibliotecas de manipulacao dos formatos XML e XSD
# MAGIC import com.databricks.spark.xml._ 
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC import com.databricks.spark.xml.util.XSDToSchema
# MAGIC import java.nio.file.Paths
# MAGIC import spark.implicits._

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // primeiro faz a leitura da estrutura do XML (Schema layout)
# MAGIC val schema_xsd = XSDToSchema.read(Paths.get("dbfs:/user/luis_assuncao/db_tax_lakehouse/leiauteNFe_v4.00.xsd"))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // depois utiliza o "schema" lido para carregar a estrutura do XML
# MAGIC val df = spark.read
# MAGIC    .schema(schema_xsd)
# MAGIC    .xml(working_dir + "nfe.xml")
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC % scala
# MAGIC 
# MAGIC val df = ... /// DataFrame with XML in column 'payload'
# MAGIC val payloadSchema = schema_of_xml(df.select("payload").as[String])
# MAGIC val parsed = df.withColumn("parsed", from_xml($"payload", payloadSchema))

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read
# MAGIC   .option("rowTag", "dest")
# MAGIC   .xml("dbfs:/user/luis_assuncao_databricks_com/db_tax/nfe.xml")
# MAGIC 
# MAGIC display(df)
