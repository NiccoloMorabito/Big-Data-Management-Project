from bs4 import BeautifulSoup
import requests
from os import makedirs
from os.path import abspath, dirname, join

import urllib3
urllib3.disable_warnings()

BRAZIL_FOLDER = join(dirname(abspath(__file__)), "data/brazil")
BRAZIL_URL = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
COMMON_PART_IN_LINK = "https://balanca.economia.gov.br/balanca/bd/"

if __name__ == '__main__':
    req = requests.get(BRAZIL_URL).text
    soup = BeautifulSoup(req, features='lxml')

    for link in soup.findAll("a"):
        current_link = link.get("href")
        if current_link and current_link.endswith('csv'):
            print(current_link)
            print(f"\t{current_link.replace(COMMON_PART_IN_LINK, '')}")

            url_content = requests.get(current_link, verify=False).content

            filepath = join(BRAZIL_FOLDER, current_link.replace(COMMON_PART_IN_LINK, '') )

            makedirs(dirname(filepath), exist_ok=True)

            with open(filepath, 'wb') as csv_file:
                csv_file.write(url_content)
            
