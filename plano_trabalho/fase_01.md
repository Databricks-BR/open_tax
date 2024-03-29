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
| 1 | NF-e	| Nota Fiscal Eletrônica (XML) |	
| 2 | ECD	| SPED CONTÁBIL -Escrituração Contábil Digital |
| 2 | ECF	| Escrituração Contábil Fiscal |
| 2 | EFD	| SPED Fiscal (incluindo registros da NFCOM) |
| 2 | EFD-Contribuições | Contribuições	Escrituração Fiscal Digital - PIS/COFINS |
| 2 | EFD-Reinf	| Escrituração Fiscal Digital de Retenções e Outras Informações Fiscais |
| 3 | CT-e	| Conhecimento de Transporte Eletrônico	 |
| 9 | MDF-e	| Manifesto Eletrônico de Documentos Fiscais |
| 9 | DCTF | Declaração de Débitos e Créditos Tributários Federais |
| 9 | DIRF | Declaração do Imposto de Renda Retido na Fonte |
| 9 | PERDCOMP | Pedido de Restituição e Declaração de Compensação |
| 9 | Convênio ICMS 115   |  Telecom e Energia |
| 9 | Obrigações Estaduais de ICMS  | ??? |
| 9 | Convênio 201  | x |
| 9 | Relatório de DARF  | extraído do site do eCAC  |
| 9 | ADRC-ST  | Arquivo Digital da Recuperação, do Ressarcimento e da Complementação do ICMS-ST |
| 9 | CAT 42  | Ressarcimento de Substituição Tributária do ICMS |
| 9 | Convênio 126  | concessão de regime especial ICMS para Telecom |
| 9 | Obrigações municipais  |  ??? |
| 9 | Selic  | Sistema Especial de Liquidação e de Custódia |
| 9 | GIA | Guia de Informação e Apuração do ICMS |
| 9 | E-social  | encargos sociais |


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
| SPED | Video com excel | https://www.youtube.com/watch?v=eNSXitKONhc |
| EFD Contribuições | Layout Blocos | https://documentacao.senior.com.br/goup/5.10.2/menu_controladoria/sped/contribuicoes-pis-cofins/lucro-real/detalhamento-registros.htm |
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