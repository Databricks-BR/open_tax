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

# DBTITLE 1,Cópia dos arquivos de origem (do GitHub)
# MAGIC %scala
# MAGIC 
# MAGIC // Faz a copia dos arquivos TESTE a serem carregados (Origem:  Github.com/Databricks-BR).
# MAGIC // Esse passo, em produção, deve ser substituído para fazer a ingestão direto
# MAGIC // no repositório das Notas Fiscais da empresa
# MAGIC 
# MAGIC // bibliotecas
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC 
# MAGIC // Cria um subdiretorio temporario para cópia
# MAGIC dbutils.fs.mkdirs("/tmp/tax_lakehouse")
# MAGIC 
# MAGIC // inicializacao de variaveis (constantes)
# MAGIC // localizacao dos arquivos de entrada e saida
# MAGIC var github = "https://raw.githubusercontent.com/Databricks-BR/tax_lakehouse/main/datasets/"
# MAGIC var working_dir = "dbfs:/user/luis_assuncao/tax_lakehouse/"
# MAGIC var tmpPath = "/tmp/tax_lakehouse/"
# MAGIC var tmpFileXML = tmpPath + "nfe.xml"
# MAGIC var dbfsFileXML = working_dir + "nfe.xml"
# MAGIC 
# MAGIC // faz a copia do GitHub de origem para um diretorio temporario 
# MAGIC FileUtils.copyURLToFile(new URL(github + "NFe_XML/NFe_001.xml"), new File(tmpFileXML))
# MAGIC 
# MAGIC // faz a copia para o DBFS (Databricks File System)
# MAGIC dbutils.fs.cp( "file://" + tmpFileXML, dbfsFileXML )
# MAGIC 
# MAGIC // faz a copia dos arquivos de estrutura do XML (schema)
# MAGIC var fileXSD = new Array[String](3)
# MAGIC fileXSD(0) = "leiauteNFe_v4.00.xsd"            // Layout principal da Nota Fiscal versao 4.00
# MAGIC fileXSD(1) = "xmldsig-core-schema_v1.01.xsd"   // Layout chamado (import) dentro do layout principal
# MAGIC fileXSD(2) = "tiposBasico_v4.00.xsd"           // Layout chamado (include) dentro do layout principal
# MAGIC 
# MAGIC for( iXSD <- 0 to 2){
# MAGIC       FileUtils.copyURLToFile(new URL(github + "XSD_schema/" + fileXSD(iXSD) ), new File(tmpPath + fileXSD(iXSD) ));
# MAGIC       dbutils.fs.cp( "file://" + tmpPath + fileXSD(iXSD) , working_dir + fileXSD(iXSD)  ) ;  
# MAGIC       println("Arquivo: " + fileXSD(iXSD));
# MAGIC    }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark dbutils
# MAGIC Comando de manipulação de arquivos e diretórios.
# MAGIC Nesse passo utilizado para verificação dos processos de cópia, e visualização do contéudo original dos arquivos.
# MAGIC **Referência:**
# MAGIC * https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs

# COMMAND ----------

# DBTITLE 1,Verificação dos arquivos (passo de teste)
# MAGIC %python
# MAGIC 
# MAGIC #dbutils.fs.ls("file:/tmp/tax_lakehouse/")                 # Lista os arquivos do diretorio Temporario
# MAGIC # dbutils.fs.head("file:/tmp/tax_lakehouse/nfe.xml")       # leitura do conteudo inicial do arquivo
# MAGIC 
# MAGIC dbutils.fs.ls(f"dbfs:/user/luis_assuncao/tax_lakehouse/") # Lista os arquivos do Databricks File System
# MAGIC # dbutils.fs.head("dbfs:/user/luis_assuncao/tax_lakehouse/nfe.xml")

# COMMAND ----------

# DBTITLE 1,Bibliotecas de manipulação de XML e XSD
# MAGIC %scala
# MAGIC 
# MAGIC import com.databricks.spark.xml._ 
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC import com.databricks.spark.xml.util.XSDToSchema
# MAGIC import java.nio.file.Paths
# MAGIC import spark.implicits._

# COMMAND ----------

# DBTITLE 1,Leitura do Schema (layout do XML)
# MAGIC %scala
# MAGIC 
# MAGIC // arquivo contendo o schema (estrutura aninhada do XML)
# MAGIC var schema_name = working_dir + fileXSD(0)
# MAGIC 
# MAGIC // primeiro faz a leitura da estrutura do XML (Schema layout)
# MAGIC val schema_xsd = XSDToSchema.read(Paths.get(schema_name))
# MAGIC 
# MAGIC println(schema_xsd)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // depois utiliza o "schema" lido para carregar a estrutura do XML
# MAGIC val df = spark.read
# MAGIC    .schema(schema_xsd)
# MAGIC    .option("rowTag", "dest")
# MAGIC    .xml(working_dir + "nfe.xml")
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read
# MAGIC   .option("rowTag", "dest")
# MAGIC   .xml(working_dir + "nfe.xml")
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df.printSchema()
