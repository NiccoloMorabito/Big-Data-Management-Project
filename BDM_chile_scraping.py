from bs4 import BeautifulSoup
import requests
import os
from rarfile import RarFile
import re

import urllib3
urllib3.disable_warnings()

CHILE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/chile")
CHILE_URL = "https://datos.gob.cl/organization/servicio_nacional_de_aduanas?page={}"
HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'}

def get_new_datasets(existing_folders):
    datasets = get_datasets_from_all_pages()
    print(f"Folders that are going to be ignored because already downloaded:")
    print({dataset for dataset in datasets if dataset.split('/')[2] in existing_folders})
    return {dataset for dataset in datasets if dataset.split("/")[2] not in existing_folders}

def get_datasets_from_all_pages():
    # collect the different datasets from the main site by iterating on the different pages
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

def extract_rars_in(folderpath):
    '''
    Extracts all rar files in the folder and deletes the rar files afterwards
    '''
    for file in os.listdir(folderpath):
        #TODO check if this regex works
        if re.match("\.part[0-9][2-9]\.rar", file):
            continue
        if file.endswith(".rar"):
            filepath = os.path.join(folder, file)
            try:
                rf = RarFile(filepath)
                rf.extractall(folderpath)
            except:
                print(f"Error extracting file: {filepath}") #TODO
                continue
    
    for file in os.listdir(folderpath):
        if file.endswith(".rar"):
            filepath = os.path.join(folder, file)
            os.remove(filepath)

if __name__ == '__main__':
    #TODO run this every x days
    
    #TODO this code to check what data is already downloaded won't work in the temporary landing setting
    if os.path.isdir(CHILE_FOLDER):
        existing_folders = set(os.listdir(CHILE_FOLDER))
    else:
        existing_folders = set()
    new_datasets = get_new_datasets(existing_folders)

    for dataset in new_datasets:
        current_link = "https://datos.gob.cl" + dataset
        print(current_link)

        url_content = requests.get(current_link, verify=False, headers=HEADER).content
        subsoup = BeautifulSoup(url_content, features='lxml')
        for sublink in subsoup.findAll("a"):
            download_link = sublink.get("href")
            if download_link and (download_link.endswith('xlsx') or download_link.endswith('rar')):
                file_content = requests.get(download_link, verify=False, headers=HEADER).content
                # filepath is the CHILE_FOLDER + folder specifying import/export and year + file name
                filepath = os.path.join(CHILE_FOLDER, current_link.split("/")[-1], download_link.split("/")[-1])
                print("\t" + filepath)
                
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'wb') as file:
                    file.write(file_content)
    
    # extract RAR files
    print("\n\nExtracting...")
    for file in os.listdir(CHILE_FOLDER):
        folder = os.path.join(CHILE_FOLDER, file)
        if os.path.isdir(folder):
            print("Working with: " + folder)
            extract_rars_in(folder)

