import json
import requests
from bs4 import BeautifulSoup
from typing import List

def load_seen_files(json_path: str) -> List[str]:
    try:
        with open(json_path, 'r+', encoding='utf8') as file:
            return json.load(file)
    except:
        return list()

def save_seen_files(json_path: str, files: List[str]):
    with open(json_path, 'w+', encoding='utf8') as file:
        json.dump(files, file)

def get_links_at(url) -> List[str]:
    req = requests.get(url).text
    soup = BeautifulSoup(req, features='lxml')

    links = soup.findAll("a")
    return [link.get("href") for link in links]