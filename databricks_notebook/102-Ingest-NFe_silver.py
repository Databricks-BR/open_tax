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
# MAGIC | **Objetivo Pipeline** | Ingestão Nota Fiscal Eletrônica - NFe - XML |
# MAGIC | **Camada** | Bronze para SILVER |
# MAGIC | **Databricks Run Time** | DBR 14.3 LTS |
# MAGIC | **Linguagem** | SQL |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código (Pipeline)
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 12-ABR-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão  |

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.nfe_prod 
# MAGIC as
# MAGIC    select _id as num_NFe, produto.*
# MAGIC from
# MAGIC (
# MAGIC    select _id, explode(det.prod) as produto
# MAGIC    from tax.bronze.nfe_xml2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tax.silver.nfe_prod

# COMMAND ----------

# MAGIC %sql
# MAGIC select _id as id_nfe,
# MAGIC _versao as versao_nfe,
# MAGIC ide.cUF as uf,
# MAGIC ide.cNF as cod_chave_acesso,
# MAGIC ide.natOp as cod_nat_operacao,
# MAGIC ide.indPag as ind_forma_pag,
# MAGIC ide.mod as cod_modelo_fiscal,
# MAGIC ide.serie as num_serie,
# MAGIC ide.nNF as num_doc_fiscal,
# MAGIC ide.dhEmi as dt_emissao,
# MAGIC ide.dhSaiEnt as dt_saida_entrada,
# MAGIC ide.tpNF as tip_operacao,
# MAGIC ide.cMunFG as cod_mun_fato_gerador,
# MAGIC ide.tpImp as tpImp,
# MAGIC ide.tpEmis as tpEmis,
# MAGIC ide.cDV as cDV,
# MAGIC ide.tpAmb as tpAmb,
# MAGIC ide.finNFe as finNFe,
# MAGIC ide.indFinal as indFinal,
# MAGIC ide.indPres as indPres,
# MAGIC emit.CNPJ as emit_cnpj,
# MAGIC emit.xNome as emit_xnome,
# MAGIC emit.xFant as emit_xfant
# MAGIC --emit.enderEmit.xLgr as emit_end_logradouro,
# MAGIC --emit.enderEmit.nro as emit_end_nro,
# MAGIC --emit.enderEmit.xCpl as emit_end_compl,
# MAGIC --emit.enderEmit.xBairro as emit_end_bairro,
# MAGIC --emit.enderEmit.cMun as emit_end_cmun,
# MAGIC --emit.enderEmit.xMun as emit_end_xmun,
# MAGIC --emit.enderEmit.UF as emit_end_uf,
# MAGIC --emit.enderEmit.CEP as emit_end_cep,
# MAGIC --emit.enderEmit.cPais as emit_end_cpais,
# MAGIC --emit.enderEmit.xPais as emit_end_xpais,
# MAGIC --emit.fone as emit_end_fone,
# MAGIC --emit.fone as emit_end_fone,
# MAGIC --emit.IE as emit_insc_estadual,
# MAGIC --emit.IEST as emit_insc_estadual_st
# MAGIC from tax.bronze.nfe_xml2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG tax;
# MAGIC USE SCHEMA silver;
# MAGIC
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_nfe COMMENT 'TAG raiz da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN infNFe COMMENT 'Informações da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN versao_layout COMMENT 'Versão do leiaute';
# MAGIC ALTER TABLE nfe ALTER COLUMN id_nfe COMMENT 'Identificador da TAG a ser assinada';
# MAGIC ALTER TABLE nfe ALTER COLUMN pk_nItem COMMENT 'Regra para que a numeração do item de detalhe da NF-e
# MAGIC seja única.';
# MAGIC ALTER TABLE nfe ALTER COLUMN ide COMMENT 'Informações de identificação da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN uf COMMENT 'Código da UF do emitente do Documento Fiscal';
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_chave_acesso COMMENT 'Código Numérico que compõe a Chave de Acesso';
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_nat_operacao COMMENT 'Descrição da Natureza da Operação';
# MAGIC ALTER TABLE nfe ALTER COLUMN ind_forma_pag COMMENT 'Indicador da forma de pagamento';
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_modelo_fiscal COMMENT 'Código do Modelo do Documento Fiscal';
# MAGIC ALTER TABLE nfe ALTER COLUMN num_serie COMMENT 'Série do Documento Fiscal';
# MAGIC ALTER TABLE nfe ALTER COLUMN num_doc_fiscal COMMENT 'Número do Documento Fiscal';
# MAGIC ALTER TABLE nfe ALTER COLUMN dt_emissao COMMENT 'Data e hora de emissão do Documento Fiscal';
# MAGIC ALTER TABLE nfe ALTER COLUMN dt_saida_entrada COMMENT 'Data e hora de Saída ou da Entrada da Mercadoria/Produto';
# MAGIC ALTER TABLE nfe ALTER COLUMN tip_operacao COMMENT 'Tipo de Operação';
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_mun_fato_gerador COMMENT 'Código do Município de Ocorrência do Fato Gerador';
# MAGIC ALTER TABLE nfe ALTER COLUMN tpImp COMMENT 'Formato de Impressão do DANFE';
# MAGIC ALTER TABLE nfe ALTER COLUMN tpEmis COMMENT 'Tipo de Emissão da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN cDV COMMENT 'Dígito Verificador da Chave de Acesso da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN tpAmb COMMENT 'Identificação do Ambiente';
# MAGIC ALTER TABLE nfe ALTER COLUMN finNFe COMMENT 'Finalidade de emissão da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN indFinal COMMENT 'Indica operação com Consumidor final';
# MAGIC ALTER TABLE nfe ALTER COLUMN indPres COMMENT 'Indicador de presença do comprador no estabelecimento comercial no momento da operação';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit COMMENT 'Identificação do emitente da NF-e';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_cnpj COMMENT 'CNPJ do emitente';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_xnome COMMENT 'Razão Social ou Nome do emitente';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_xfant COMMENT 'Nome fantasia';
# MAGIC ALTER TABLE nfe ALTER COLUMN enderEmit COMMENT 'Endereço do emitente';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_logradouro COMMENT 'Logradouro do emitente';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_nro COMMENT 'Número';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_compl COMMENT 'Complemento';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_bairro COMMENT 'Bairro';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_cmun COMMENT 'Código do município';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_xmun COMMENT 'Nome do município';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_uf COMMENT 'Sigla da UF';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_cep COMMENT 'Código do CEP';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_cpais COMMENT 'Código do País';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_xpais COMMENT 'Nome do País';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_end_fone COMMENT 'Telefone';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_insc_estadual COMMENT 'Inscrição Estadual do Emitente';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_insc_estadual_st COMMENT 'IE do Substituto Tributário';
# MAGIC ALTER TABLE nfe ALTER COLUMN emit_insc_municipal COMMENT 'Inscrição Municipal do Prestador de Serviço';
# MAGIC ALTER TABLE nfe ALTER COLUMN cod_cnae COMMENT 'CNAE fiscal';
