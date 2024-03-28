# PLANO DE TRABALHO - FASE 1


## OBJETIVO da Fase 1

* Coleta e Pesquisa de materiais que possam ser utilizados como aceleradores do projeto.
* Coleta e/ou criação de Bases de Dados de Testes (com dados fictícios).
* Listagem das principais fontes de informação (dados de origem) para o Data Lake.

</br>

## Macro Cronograma

| Atividade | Data Alvo | Progresso |
| -- | -- | --: |
| 1) Levantamento de materiais públicos - GitHub | 30/03/2024 | 50% |
| 2) Levantamento de códigos-fonte dos colaboradores | 20/04/2024 | 0% |
| 3) Levantamento de base de dados para teste | 20/04/2024 | 0% |
| 4) Organização do material levantado | 30/04/2024 | 0% |

</br></br>

## Mapeamento de Fontes de Dados para o Data Lake

### Documentos Digitais

| Pri. | Fonte | Descrição |
| -- | -- | -- |
| 1 | NF-e	| Nota Fiscal Eletrônica |	
| 2 | NFS-e	| Nota Fiscal de Serviços Eletrônica| 
| 2 | CT-e	| Conhecimento de Transporte Eletrônico para Outros Serviços |
| 2 | ECD	| Escrituração Contábil Digital |
| 2 | ECF	| Escrituração Contábil Fiscal |
| 2 | EFD | Contribuições	Escrituração Fiscal Digital - PIS/COFINS |
| 2 | EFD-Reinf	| Escrituração Fiscal Digital de Retenções e Outras Informações Fiscais |
| 3 | CT-e	| Conhecimento de Transporte Eletrônico	 |
| 8 | NFC-e	| Nota Fiscal de Consumidor Eletrônica |
| 9 | MDF-e	| Manifesto Eletrônico de Documentos Fiscais |
| 9 | CC-e	| Carta de Correção Eletrônica |

### Tabelas Auxiliares de Tipificação (Códigos)

| Pri. | Fonte | Descrição |
| -- | -- | -- |
| 1 | CFOP	| Código Fiscal de Operações e Prestações  |
| 1 | NCM	| Nomenclatura Comum do Mercosul  |
| 2 | CNAE	| Classificação Nacional de Atividades Econômicas |
| 3 | CSOSN | Código de Situação da Operação no Simples Nacional |
| 3 | CST | Código de Situação Tributária |
| 3 | CEST	| Código Especificador da Substituição Tributária |
 
</br></br>

## LINKS públicos - Potenciais Aceleradores de Codificação

| status | obj. | URL | observações |
| -- | -- | -- | -- |
| ruim | NFe | https://github.com/marinho/PyNFe | codigo pra criação da NFe |
| ruim |  NFe | https://github.com/TadaSoftware/PyNFe  | codigo pra criação da NFe |
| OK |  NFe | https://github.com/akretion/nfelib | parece util |
| OK |  NFe | https://github.com/3bears-data/ler-xml-nfe-python/blob/main/main.py | parece util - tem layout |
| ? |  PySPED	|https://pypi.org/project/python-sped/ | avaliar |
| ? |  PySPED	| https://github.com/aricaldeira/PySPED/tree/python3 | avaliar |


</br></br>
## Documentações de Apoio

| Item | Descrição | URL |
| -- | -- | -- |
| SPED | Doc Governo | http://sped.rfb.gov.br |
| SPED |	Doc Governo | http://sped.rfb.gov.br/pasta/show/1495 |
| SPED |	Doc Governo | 	http://sped.rfb.gov.br/pasta/show/1492 |
| SPED |	Doc Governo | 	https://portal.fazenda.sp.gov.br/servicos/sped/Paginas/Sobre.aspx |
| SPED |	Exemplo SPED	 | http://sped.rfb.gov.br/pasta/show/1606 |
| SPED | Artigo Medium Fugimura	| https://fugimura.medium.com/quebra-de-speds-com-python-79d9648b3772 |
| NFe	| guia | https://www.smartdocx.com.br/blog/guia-nota-fiscal-eletronica |
| NFe	| blog | https://nfe.io/blog/nota-fiscal/como-conseguir-arquivo-xml-nota-fiscal |

</br></br>
## LINKS técnicos 

| Item | Descrição | URL |
| -- | -- | -- |
| XML | spark read | https://medium.com/@uzzaman.ahmed/working-with-xml-files-in-pyspark-reading-and-writing-data-d5e570c913de |
| XML | python | https://awari.com.br/xml-aprenda-a-ler-e-manipular-arquivos-xml-com-python |
| XSD | databricks | https://learn.microsoft.com/pt-br/azure/databricks/data/data-sources/xml#xsd-support |
| XML | databricks | https://learn.microsoft.com/en-us/azure/databricks/query/formats/xml |
| XML | xlm schema	| https://pypi.org/project/xmlschema |


</br></br></br></br>
