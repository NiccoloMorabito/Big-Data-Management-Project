from bs4 import BeautifulSoup
import requests
from os import makedirs
from os.path import dirname, join
from json_utils import load_seen_files, save_seen_files

import urllib3
urllib3.disable_warnings()

BRAZIL_FOLDER = "../data/brazil"
BRAZIL_URL = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
COMMON_PART_IN_LINK = "https://balanca.economia.gov.br/balanca/bd/"
BRAZIL_SEENFILES_PATH = "brazil.json"

def get_all_links_in(url):
    req = requests.get(url).text
    soup = BeautifulSoup(req, features='lxml')

    return soup.findAll("a")

if __name__ == '__main__':
    #TODO run this every x days

    seen_files = load_seen_files(BRAZIL_SEENFILES_PATH)

    for link in get_all_links_in(BRAZIL_URL):
        current_link = link.get("href")
        if current_link and (current_link.endswith('.csv') or current_link.endswith('.xlsx')):
            dataset_name = current_link.replace(COMMON_PART_IN_LINK, '')
            if dataset_name in seen_files:
                continue

            filepath = join(BRAZIL_FOLDER, dataset_name)
            makedirs(dirname(filepath), exist_ok=True)
            url_content = requests.get(current_link, verify=False).content
            with open(filepath, 'wb') as csv_file:
                csv_file.write(url_content)

            print(f"{current_link}\t({dataset_name})")
            
            seen_files.append(dataset_name)

    save_seen_files(BRAZIL_SEENFILES_PATH, seen_files)
