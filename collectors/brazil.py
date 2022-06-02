import os.path

from collectors.scraper_collector import ScraperCollector

import urllib3

urllib3.disable_warnings()


def main():
    collector = ScraperCollector(
        'https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta',
        '',
        'brazil')
    links = collector.get_unseen_links()
    for link in links:
        filename = os.path.basename(link)
        if not filename.endswith(('.csv', 'xlsx')):
            continue
        file = collector.download_file(link)
        collector.upload_to_hdfs(file)


if __name__ == '__main__':
    main()
