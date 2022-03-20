from bs4 import BeautifulSoup
import requests
from os import makedirs
from os.path import abspath, dirname, join
import patoolib

import urllib3
urllib3.disable_warnings()

CHILE_FOLDER = join(dirname(abspath(__file__)), "data/chile")
CHILE_URL = "https://datos.gob.cl/organization/servicio_nacional_de_aduanas?page={}"

if __name__ == '__main__':
    #TODO change names of variables to make code more understandable
    HDR = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'}

    # collect the different datasets from the main site by iterasting on the different pages
    datasets = set()
    page=1
    while True:
        req = requests.get(CHILE_URL.format(page), headers=HDR).text
        soup = BeautifulSoup(req, features='lxml')
        new_datasets = {link.get("href") for link in soup.findAll("a") if link.get("href").startswith("/dataset/")}
        if not new_datasets:
            break
        datasets |= new_datasets
        page += 1
    
    for dataset in datasets:
        current_link = "https://datos.gob.cl" + dataset
        print(current_link)

        url_content = requests.get(current_link, verify=False, headers=HDR).content
        subsoup = BeautifulSoup(url_content, features='lxml')
        for sublink in subsoup.findAll("a"):
            download_link = sublink.get("href")
            if download_link and (download_link.endswith('xlsx') or download_link.endswith('rar')):
                file_content = requests.get(download_link, verify=False, headers=HDR).content
                # filepath is the CHILE_FOLDER + folder specifying import/export and year + file name
                filepath = join(CHILE_FOLDER, current_link.split("/")[-1], download_link.split("/")[-1])
                print("\t" + filepath)
                
                makedirs(dirname(filepath), exist_ok=True)
                with open(filepath, 'wb') as file:
                    file.write(file_content)
                '''
                RAR FILES NEED TO BE EXTRACTED
                if filepath.endswith(".rar"):
                    patoolib.extract_archive(filepath, outdir=filepath.split("/")[:-1])
                '''

