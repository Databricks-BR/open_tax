-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/header_opentax.png" width="800px">
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Controle de Versão do Código (Pipeline)
-- MAGIC
-- MAGIC | versão | data | autor | e-mail | alterações |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | 1.0 | 12-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Descrição e Objetivos desse Pipeline
-- MAGIC
-- MAGIC | projeto | aplicação | módulo | tabela | objetivo |
-- MAGIC | --- | --- | --- | --- | --- |
-- MAGIC | Open TAX | Tax Lakehouse | Setup INICIAL | Criação do Catálogo | SETUP inicial - ONE TIME |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://www.databricks.com/glossary/medallion-architecture">
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/medalhao.png" width="800px"></a>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,CRIAÇÃO DO CATÁLOGO

CREATE CATALOG tax;

COMMENT ON CATALOG tax IS 'Lakehouse Tributário, para apoio gerencial aos processos fiscais, visando a melhoria contínua, identificação de falhas (Tax Compliance), modelos inteligentes de identificação de oportunidades (Tax Transformation) e democratização das informações fiscais.';



-- COMMAND ----------

-- DBTITLE 1,CRIAÇÃO DOS LAYERS (Bronze, Silver, Gold)
USE CATALOG tax;

CREATE SCHEMA landing;
COMMENT ON SCHEMA landing IS 'Camada LANDING (staging) armazena os arquivos ORIGINAIS no seu formato nativo (CSV, XML, JSON)';

CREATE SCHEMA bronze;
COMMENT ON SCHEMA bronze IS 'Camada BRONZE (raw) armazena todos os dados BRUTOS de sistemas de origem, procurando manter o layout original também, permite pesquisa no dado original, linhagem de dados, auditoria e reprocessamento conforme necessário, sem recarregar os dados do sistema de origem.(Ex. Notas Fiscais XML sem tratamento)';

CREATE SCHEMA silver;
COMMENT ON SCHEMA silver IS 'Camada SILVER (curated) armazena os dados já trabalhados, tratados e enriquecidos, originados na camada Bronze.  As entidades (tabelas) armazenadas nessa camada apoiam as equipe de dados na construção de visões, cruzamentos de dados, análise exploratória em uma granularidade de dados detalhada. Dados nessa camada podem ser utilizados nos Modelos de Inteligência Artificial.';

CREATE SCHEMA gold;
COMMENT ON SCHEMA gold IS 'Camada GOLD (business) armazena dados resultantes de cruzamentos e Modelos de IA. Normamelte são dados preparados para consumo das áreas de negócio, através de soluções de Visualização de Dados e Painéis. Geralmente são agregados, e por contexto do negócio. (Ex. Resultado da análise de outliers, potenciais erros de tributação, Indicadores Tributários, Cruzamentos e Análises Fiscais).';

CREATE SCHEMA metadado;
COMMENT ON SCHEMA metadado IS 'Camada de armazenamento do LAYOUT de Arquivos de Entrada (Notas Fiscais XML, Blocos do SPED, mapeamento de Documentos Fiscais, Glossários de Termos).';

-- COMMAND ----------

-- DBTITLE 1,CRIAÇÃO DOS VOLUMES (Associados ao storage)
USE CATALOG tax;
USE SCHEMA landing;
CREATE VOLUME nfe_xml;
CREATE VOLUME sped;

