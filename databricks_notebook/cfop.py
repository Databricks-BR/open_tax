import requests
from bs4 import BeautifulSoup 
import re
# This code crawls through the Sefaz and prints all the CFOP codes using regular expressions

page = requests.get('https://www.sefaz.pe.gov.br/Legislacao/Tributaria/Documents/Legislacao/Tabelas/CFOP.htm') #URl requested 
if page.status_code == 200:
    print('Sucess crawling CFOP ' )
    content = page.content
soup = BeautifulSoup(content, 'html.parser')
spans = soup.findAll("span",{"style" : "font-family:\"Arial\",\"sans-serif\""})
listCfop = list() 
for valor in spans:
	 y = re.search("\d{1}.\d{3}",valor.get_text())
	 if(y):
		 listCfop.append(y.group())
listCfop.sort()
for item in listCfop:
	print(item)