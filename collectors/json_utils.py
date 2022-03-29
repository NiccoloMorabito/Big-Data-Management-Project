import json
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