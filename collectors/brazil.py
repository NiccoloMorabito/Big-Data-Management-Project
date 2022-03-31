from bs4 import BeautifulSoup
import requests
from os import makedirs
from os.path import dirname, join
from hdfs import InsecureClient
from json_utils import load_seen_files, save_seen_files

from signal import signal, SIGPIPE, SIG_DFL 
#Ignore SIG_PIPE and don't throw exceptions on it
signal(SIGPIPE,SIG_DFL) 

import urllib3
urllib3.disable_warnings()

BRAZIL_FOLDER = "/data/brazil"
BRAZIL_URL = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
COMMON_PART_IN_LINK = "https://balanca.economia.gov.br/balanca/bd/"
BRAZIL_SEENFILES_PATH = "brazil.json"

def get_all_links_in(url):
    req = requests.get(url).text
    soup = BeautifulSoup(req, features='lxml')

    return soup.findAll("a")

if __name__ == '__main__':
    #TODO run this every x days

    client = InsecureClient('http://127.0.0.1:9870', user='bdm')

    seen_files = load_seen_files(BRAZIL_SEENFILES_PATH)

    for link in get_all_links_in(BRAZIL_URL):
        current_link = link.get("href")
        if current_link and (current_link.endswith('.csv') or current_link.endswith('.xlsx')):
            dataset_name = current_link.replace(COMMON_PART_IN_LINK, '')
            if dataset_name in seen_files:
                continue

            filepath = join(BRAZIL_FOLDER, dataset_name)

            url_content = requests.get(current_link, verify=False).content.decode('utf-8')
            
            with client.write(filepath, encoding='utf-8') as writer:
                writer.write(url_content)

            print(f"{current_link}\t({dataset_name})")
            
            seen_files.append(dataset_name)

    save_seen_files(BRAZIL_SEENFILES_PATH, seen_files)
