from bs4 import BeautifulSoup
import requests
import os
from rarfile import RarFile
import re
from hdfs import InsecureClient
from utils import load_seen_files, save_seen_files

import urllib3
urllib3.disable_warnings()

CHILE_HDFS_FOLDER = "/data/chile"
CHILE_URL = "https://datos.gob.cl/organization/servicio_nacional_de_aduanas?page={}"
CHILE_SEENFILES_PATH = "chile.json"
HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'}

def get_datasets_from_all_pages():
    '''
    Collects the different datasets from the main site by iterating on the different pages
    '''
    page=1
    datasets = set()
    newpage_datasets = get_datasets_in(page)
    while newpage_datasets:
        datasets |= newpage_datasets
        page += 1
        newpage_datasets = get_datasets_in(page)
    
    return datasets

def get_datasets_in(page):
    url = CHILE_URL.format(page)
    req = requests.get(url, headers=HEADER).text
    soup = BeautifulSoup(req, features='lxml')
    return {link.get("href") for link in soup.findAll("a") if link.get("href").startswith("/dataset/")}

def extract_rars_in(folderpath, hdfs_client):
    '''
    Extracts all rar files in the folder putting the content in HDFS and deletes the rar files afterwards
    '''
    for file in os.listdir(folderpath):
        if re.search(r"\.part[0-9][2-9]\.rar", file):
            continue
        if file.endswith(".rar"):
            filepath = os.path.join(folderpath, file)
            try:
                rf = RarFile(filepath)
                rf.extractall(folderpath)
            except:
                print(f"Error extracting file: {filepath}") #TODO
                continue
    
    for file in os.listdir(folderpath):
        if file.endswith(".rar"):
            filepath = os.path.join(folderpath, file)
            os.remove(filepath)
    
    
    for file in os.listdir(folderpath):
        filepath = os.path.join(folderpath, file)
        hdfspath = os.path.join(CHILE_HDFS_FOLDER, filepath)
        hdfs_client.upload(hdfspath, filepath)
        os.remove(filepath)
    
    os.rmdir(folderpath)

def main():
    hdfs_client = InsecureClient('http://127.0.0.1:9870', user='bdm')
    new_datasets = get_datasets_from_all_pages()
    seen_files = load_seen_files(CHILE_SEENFILES_PATH)

    for dataset in new_datasets:
        current_link = "https://datos.gob.cl" + dataset
        print(current_link)
        rar_to_extract = False

        url_content = requests.get(current_link, verify=False, headers=HEADER).content
        subsoup = BeautifulSoup(url_content, features='lxml')
        for sublink in subsoup.findAll('a'):
            download_link = sublink.get('href')
            if not download_link or not download_link.endswith('.rar'):
                continue
            # file_name is composed of the folder specifying import/export and year + file name
            file_name = os.path.join(current_link.split('/')[-1], download_link.split('/')[-1])
            if file_name in seen_files:
                continue
            print("\t" + file_name)

            rar_to_extract = True
            seen_files.append(file_name)
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

            file_content = requests.get(download_link, verify=False, headers=HEADER).content
            with open(file_name, 'wb') as file:
                file.write(file_content)
            
        # extract all rar files in temp
        if rar_to_extract:
            print(f"Extracting {os.path.dirname(file_name)}...")
            extract_rars_in(os.path.dirname(file_name), hdfs_client)
        
        save_seen_files(CHILE_SEENFILES_PATH, seen_files)

if __name__ == '__main__':
    main()