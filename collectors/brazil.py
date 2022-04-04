import requests
from os.path import join
from hdfs import InsecureClient

from settings.env import HDFS_SERVER, HDFS_USERNAME
from utils import load_seen_files, save_seen_files, get_links_at

from signal import signal, SIGPIPE, SIG_DFL

# Ignore SIG_PIPE and don't throw exceptions on it
signal(SIGPIPE, SIG_DFL)

import urllib3

urllib3.disable_warnings()

BRAZIL_HDFS_FOLDER = "/data/brazil"
BRAZIL_URL = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
COMMON_PART_IN_URL = "https://balanca.economia.gov.br/balanca/bd/"
BRAZIL_SEENFILES_PATH = "brazil.json"


def main():
    hdfs_client = InsecureClient(HDFS_SERVER, user=HDFS_USERNAME)
    seen_files = load_seen_files(BRAZIL_SEENFILES_PATH)
    links = get_links_at(BRAZIL_URL)

    for link in links:
        if link and (link.endswith('.csv') or link.endswith('.xlsx')):
            dataset_name = link.replace(COMMON_PART_IN_URL, '')
            if dataset_name in seen_files:
                continue

            filepath = join(BRAZIL_HDFS_FOLDER, dataset_name)
            url_content = requests.get(link, verify=False).content.decode('utf-8')
            with hdfs_client.write(filepath, encoding='utf-8') as writer:
                writer.write(url_content)

            print(f"{link}\t({dataset_name})")

            seen_files.append(dataset_name)

    save_seen_files(BRAZIL_SEENFILES_PATH, seen_files)


if __name__ == '__main__':
    main()
