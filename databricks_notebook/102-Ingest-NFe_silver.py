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

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/open_tax/main/images/etl_nfe.png" width="900px">

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.nfe_prod 
# MAGIC as
# MAGIC select 
# MAGIC       _id as id_nfe, 
# MAGIC       prod.cProd as cod_produto,
# MAGIC       prod.xProd as desc_produto,
# MAGIC       prod.NCM as cod_ncm,
# MAGIC       ncm.desc_ncm as desc_ncm,
# MAGIC       prod.CFOP as cod_cfop,
# MAGIC       cfop.desc_cfop as desc_cfop,
# MAGIC       prod.uCom as unid_comercial,
# MAGIC       prod.qCom as qde_comercial,
# MAGIC       prod.vProd as val_bruto_produto_servico,
# MAGIC       prod.cEAN as cod_ean,
# MAGIC       prod.cEANTrib as cod_ean_trib,
# MAGIC       prod.uTrib as unid_tributavel,
# MAGIC       prod.qTrib as qde_tributavel
# MAGIC from
# MAGIC (
# MAGIC    select _id, explode(det.prod) as prod
# MAGIC    from tax.bronze.nfe_xml
# MAGIC ) 
# MAGIC
# MAGIC left join tax.silver.tab_cfop cfop
# MAGIC on prod.CFOP = cfop.cod_cfop
# MAGIC
# MAGIC left join tax.silver.tab_ncm ncm
# MAGIC on prod.NCM = ncm.cod_ncm;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE catalog tax;
# MAGIC USE silver;
# MAGIC
# MAGIC COMMENT ON TABLE nfe_prod IS 'Nota Fiscal Eletrônica, Detalhamento de Produtos e Serviços - TAG de Produtos (det.prod)';
# MAGIC
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN id_nfe COMMENT 'Número da Nota Fiscal Eletrônica. Chave de cruzamento (FK)';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN cod_produto COMMENT 'Código do produto ou serviço';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN cod_ean COMMENT 'GTIN (Global Trade Item Number) do produto, antigo código EAN ou código de barras';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN desc_produto COMMENT 'Descrição do produto ou serviço';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN cod_ncm COMMENT 'Código NCM com 8 dígitos';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN desc_ncm COMMENT 'Categoria do Produto. Descrição do NCM (Nomenclatura Comum do Mercosul)';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN cod_cfop COMMENT 'Código Fiscal de Operações e Prestações';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN desc_cfop COMMENT 'Descrição do Código Fiscal de Operações e Prestações';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN unid_comercial COMMENT 'Unidade Comercial';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN qde_comercial COMMENT 'Quantidade Comercial';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN val_bruto_produto_servico COMMENT 'Valor Total Bruto dos Produtos ou Serviços.';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN cod_ean_trib COMMENT 'GTIN (Global Trade Item Number) da unidade tributável, antigo código EAN ou código de barras';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN unid_tributavel COMMENT 'Unidade Tributável';
# MAGIC ALTER TABLE nfe_prod ALTER COLUMN qde_tributavel COMMENT 'Quantidade Tributável';
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,NFe - informações de identificação da nota
# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.nfe_ide 
# MAGIC as 
# MAGIC select 
# MAGIC     _id as id_nfe,
# MAGIC     _versao as versao_layout, 
# MAGIC     ide.cUF as num_uf,
# MAGIC     ide.cNF as cod_chave_acesso,
# MAGIC     ide.natOp as cod_nat_operacao,
# MAGIC     ide.indPag as ind_forma_pag,
# MAGIC     ide.mod as cod_modelo_fiscal,
# MAGIC     ide.serie as num_serie,
# MAGIC     ide.nNF as num_doc_fiscal,
# MAGIC     ide.dhEmi as dt_emissao,
# MAGIC     ide.dhSaiEnt as dt_saida_entrada,
# MAGIC     ide.tpNF as cod_tipo_operacao,
# MAGIC     ide.cMunFG as cod_mun_fato_gerador,
# MAGIC     ide.tpImp as cod_tipo_impressao_danfe,
# MAGIC     ide.tpEmis as cod_tipo_emissao_nf,
# MAGIC     ide.cDV as num_dv_chave_acesso,
# MAGIC     ide.tpAmb as tip_ambiente,
# MAGIC     ide.finNFe as cod_finalidade_nfe,
# MAGIC     ide.indFinal as ind_oper_consumidor_final,
# MAGIC     ide.indPres as ind_presencao_comprador
# MAGIC     from 
# MAGIC     tax.bronze.nfe_xml;
# MAGIC

# COMMAND ----------

# DBTITLE 1,NFe Emitente
# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.nfe_emit
# MAGIC as 
# MAGIC select 
# MAGIC     _id as id_nfe
# MAGIC     ,emit.CNPJ as emit_cnpj
# MAGIC     ,emit.xNome as emit_nome_emitente
# MAGIC     ,emit.xFant as emit_nome_fantasia
# MAGIC     ,emit.enderEmit.xLgr as emit_end_logradouro
# MAGIC     ,emit.enderEmit.nro as emit_end_nro
# MAGIC     ,get_json_object(to_json(emit.enderEmit), '$.xCpl') as emit_end_complemento,
# MAGIC     ,emit.enderEmit.xBairro as emit_end_bairro
# MAGIC     ,emit.enderEmit.cMun as emit_end_cod_municipio
# MAGIC     ,emit.enderEmit.xMun as emit_end_municipio
# MAGIC     ,emit.enderEmit.UF as emit_end_uf
# MAGIC     ,emit.enderEmit.CEP as emit_end_cep
# MAGIC     ,emit.enderEmit.cPais as emit_end_cod_pais
# MAGIC     ,emit.enderEmit.xPais as emit_end_pais
# MAGIC     ,emit.enderEmit.fone as emit_end_fone
# MAGIC     ,emit.IE as emit_insc_estadual
# MAGIC     ,get_json_object(to_json(emit), '$.IEST') as emit_insc_estatual_st
# MAGIC     ,get_json_object(to_json(emit), '$.IM') as emit_insc_municipal
# MAGIC     ,get_json_object(to_json(emit), '$.CNAE') as cod_cnae
# MAGIC from 
# MAGIC     tax.bronze.nfe_xml;
# MAGIC

# COMMAND ----------

# DBTITLE 1,NFe Destinatário
# MAGIC %sql
# MAGIC
# MAGIC create or replace table tax.silver.nfe_dest
# MAGIC as 
# MAGIC select 
# MAGIC     _id as id_nfe
# MAGIC     ,get_json_object(to_json(dest), '$.CNPJ') as dest_cnpj
# MAGIC     ,get_json_object(to_json(dest), '$.CPF') as dest_cpf
# MAGIC     ,dest.xNome as dest_nome_destinatario
# MAGIC     ,dest.enderDest.xLgr as dest_end_logradouro
# MAGIC     ,dest.enderDest.nro as dest_end_num
# MAGIC     ,get_json_object(to_json(dest.enderDest), '$.xCpl') as dest_end_complemento
# MAGIC     ,dest.enderDest.xBairro as dest_end_bairro
# MAGIC     ,dest.enderDest.cMun as dest_end_cod_municipio
# MAGIC     ,dest.enderDest.xMun as dest_end_municipio
# MAGIC     ,dest.enderDest.UF as dest_end_uf
# MAGIC     ,dest.enderDest.CEP as dest_end_cep
# MAGIC     ,dest.enderDest.cPais as dest_end_cod_pais
# MAGIC     ,dest.enderDest.xPais as dest_end_pais
# MAGIC     ,dest.enderDest.fone as dest_end_fone
# MAGIC     ,dest.IE as dest_insc_estadual
# MAGIC     ,get_json_object(to_json(dest), '$.ISUF') as dest_insc_suframa 
# MAGIC     ,get_json_object(to_json(dest), '$.IM') as dest_insc_municipal
# MAGIC from 
# MAGIC     tax.bronze.nfe_xml;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC USE catalog tax;
# MAGIC USE silver;
# MAGIC
# MAGIC COMMENT ON TABLE nfe_ide IS 'Nota Fiscal Eletrônica, Informações de IDENTIFICAÇÃO da Nfe - TAG de Produtos (ide)';
# MAGIC COMMENT ON TABLE nfe_emit IS 'Nota Fiscal Eletrônica, Informações do EMISSOR (emitente) da Nfe - TAG de Produtos (emit)';
# MAGIC COMMENT ON TABLE nfe_dest IS 'Nota Fiscal Eletrônica, Informações de DESTINATÁRIO da Nfe - TAG de Produtos (dest)';
# MAGIC
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN id_nfe COMMENT 'Número da Nota Fiscal Eletrônica. Chave de cruzamento (FK)';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN id_nfe COMMENT 'Número da Nota Fiscal Eletrônica. Chave de cruzamento (FK)';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN id_nfe COMMENT 'Número da Nota Fiscal Eletrônica. Chave de cruzamento (FK)';
# MAGIC
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN versao_layout COMMENT 'Versão do leiaute';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN num_uf COMMENT 'Código da UF do emitente do Documento Fiscal';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_chave_acesso COMMENT 'Código Numérico que compõe a Chave de Acesso';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_nat_operacao COMMENT 'Descrição da Natureza da Operação';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN ind_forma_pag COMMENT 'Indicador da forma de pagamento';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_modelo_fiscal COMMENT 'Código do Modelo do Documento Fiscal';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN num_serie COMMENT 'Série do Documento Fiscal';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN num_doc_fiscal COMMENT 'Número do Documento Fiscal';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN dt_emissao COMMENT 'Data e hora de emissão do Documento Fiscal';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN dt_saida_entrada COMMENT 'Data e hora de Saída ou da Entrada da Mercadoria/Produto';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_tipo_operacao COMMENT 'Tipo de Operação';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_mun_fato_gerador COMMENT 'Código do Município de Ocorrência do Fato Gerador';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_tipo_impressao_danfe COMMENT 'Formato de Impressão do DANFE';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_tipo_emissao_nf COMMENT 'Tipo de Emissão da NF-e';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN num_dv_chave_acesso COMMENT 'Dígito Verificador da Chave de Acesso da NF-e';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN tip_ambiente COMMENT 'Identificação do Ambiente';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN cod_finalidade_nfe COMMENT 'Finalidade de emissão da NF-e';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN ind_oper_consumidor_final COMMENT 'Indica operação com Consumidor final';
# MAGIC ALTER TABLE nfe_ide ALTER COLUMN ind_presencao_comprador COMMENT 'Indicador de presença do comprador no estabelecimento comercial no momento da operação';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE catalog tax;
# MAGIC USE silver;
# MAGIC
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_cnpj COMMENT 'CNPJ do emitente';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_nome_emitente COMMENT 'Razão Social ou Nome do emitente';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_nome_fantasia COMMENT 'Nome fantasia';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_logradouro COMMENT 'Logradouro do emitente';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_nro COMMENT 'Número';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_complemento COMMENT 'Complemento';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_bairro COMMENT 'Bairro';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_cod_municipio COMMENT 'Código do município';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_municipio COMMENT 'Nome do município';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_uf COMMENT 'Sigla da UF';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_cep COMMENT 'Código do CEP';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_cod_pais COMMENT 'Código do País';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_pais COMMENT 'Nome do País';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_end_fone COMMENT 'Telefone';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_insc_estadual COMMENT 'Inscrição Estadual do Emitente';
# MAGIC ALTER TABLE nfe_emit ALTER COLUMN emit_insc_estadual_st COMMENT 'IE do Substituto Tributário';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE catalog tax;
# MAGIC USE silver;
# MAGIC
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_cnpj COMMENT 'CNPJ do destinatário';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_cpf COMMENT 'CPF do destinatário';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_nome_destinatario COMMENT 'Razão Social ou nome do destinatário';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_logradouro COMMENT 'Logradouro';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_num COMMENT 'Número';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_complemento COMMENT 'Complemento';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_bairro COMMENT 'Bairro';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_cod_municipio COMMENT 'Código do município';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_municipio COMMENT 'Nome do município';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_uf COMMENT 'Sigla da UF';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_cep COMMENT 'Código do CEP';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_cod_pais COMMENT 'Código do País';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_pais COMMENT 'Nome do País';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_end_fone COMMENT 'Telefone';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_insc_estadual COMMENT 'Inscrição Estadual do Destinatário';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_insc_suframa COMMENT 'Inscrição na SUFRAMA';
# MAGIC ALTER TABLE nfe_dest ALTER COLUMN dest_insc_municipal COMMENT 'Inscrição Municipal do Tomador do Serviço';
