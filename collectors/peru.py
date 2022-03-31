import os.path
import re

import requests

from json_utils import load_seen_files, save_seen_files


def download_file(full_link: str, destination: str):
    print('\tDownloading ...', end='')
    response = requests.get(full_link, verify=False)
    print(' Saving ...', end='')
    with open(destination, 'wb') as file:
        file.write(response.content)
    print(' Saved')


def main():
    url = 'http://www.aduanet.gob.pe/aduanas/informae/presentacion_bases_web.htm'
    base_url = 'http://www.aduanet.gob.pe'
    base_downloads_folder = './data/peru/'
    base_seenfiles_json = './collectors/peru.json'
    html = requests.get(url).text
    links = re.findall(r'href="(.+?)"', html)
    seen_files = load_seen_files(base_seenfiles_json)
    for link in links:
        filename = os.path.basename(link)
        if not filename.endswith('.zip'):
            continue
        if not filename.startswith('x'):
            continue
        if filename in seen_files:
            continue
        seen_files.append(filename)
        print(f'New file found: {filename}')
        full_link = f'{base_url}{link}'.replace('\\', '/')
        download_file(full_link, f'{base_downloads_folder}{filename}')

    save_seen_files(base_seenfiles_json, seen_files)


if __name__ == '__main__':
    main()
