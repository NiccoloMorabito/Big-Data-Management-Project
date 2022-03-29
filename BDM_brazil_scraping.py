from operator import contains
from bs4 import BeautifulSoup
import requests
from os import makedirs
from os.path import abspath, dirname, join

import urllib3
urllib3.disable_warnings()

BRAZIL_FOLDER = join(dirname(abspath(__file__)), "data/brazil")
BRAZIL_URL = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
COMMON_PART_IN_LINK = "https://balanca.economia.gov.br/balanca/bd/"

def get_all_links_in(url):
    req = requests.get(url).text
    soup = BeautifulSoup(req, features='lxml')

    return soup.findAll("a")

if __name__ == '__main__':
    #TODO run this every x days

    #TODO check what data is already downloaded not to download it again
    for link in get_all_links_in(BRAZIL_URL):
        current_link = link.get("href")
        if current_link and (current_link.endswith('.csv') or current_link.endswith('.xlsx')):
            print(current_link)
            print(f"\t{current_link.replace(COMMON_PART_IN_LINK, '')}")

            url_content = requests.get(current_link, verify=False).content

            filepath = join(BRAZIL_FOLDER, current_link.replace(COMMON_PART_IN_LINK, '') )

            makedirs(dirname(filepath), exist_ok=True)

            with open(filepath, 'wb') as csv_file:
                csv_file.write(url_content)
            
