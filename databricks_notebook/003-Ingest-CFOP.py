# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 700px"><a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_github.png" style="width: 40px; height: 40px;"></a>
# MAGIC    <a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks-br.png" style="width: 40px; height: 40px;"></a>  <a href="https://www.linkedin.com/groups/14100135"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_linkedin.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.meetup.com/pt-BR/databricks-brasil-oficial"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_meetup.png" style="height: 40px;"></a>  <a href="https://bit.ly/databricks-slack-br"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_slack.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.youtube.com/channel/UCH3cq9mit-0UkTu1mTki20Q"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_youtube.png" style="height: 38px;"></a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão da tabela de códigos da Operações Fiscais. (CFOP)
# MAGIC </br>
# MAGIC 
# MAGIC ##### Referência técnica de leitura de arquivo CSV (Separado por vírgula):
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/data/data-sources/read-csv

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
# MAGIC var tmpPath = "/tmp/tax_lakehouse/"
# MAGIC var tmpFile = tmpPath + "tabela_cfop.txt"
# MAGIC 
# MAGIC // faz a copia do GitHub de origem para um diretorio temporario 
# MAGIC FileUtils.copyURLToFile(new URL(github + "CFOP/tabela_cfop.txt"), new File(tmpFile))

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
# MAGIC dbutils.fs.head("file:/tmp/tax_lakehouse/tabela_cfop.txt")       # leitura do conteudo inicial do arquivo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE cfop
# MAGIC COMMENT "Codigos de Operacoes Fiscais"
# MAGIC AS
# MAGIC   SELECT *, 
# MAGIC     current_timestamp() updated,
# MAGIC     input_file_name() source_file
# MAGIC   FROM csv.`file:/tmp/tax_lakehouse/tabela_cfop.txt`;
# MAGIC   
# MAGIC SELECT * FROM cfop;
