import os
import os.path
import requests
from patoolib import extract_archive
from dbfread import DBF
from hdfs import InsecureClient
import csv

from settings.env import HDFS_SERVER, HDFS_USERNAME
from utils import load_seen_files, save_seen_files, get_links_at

PERU_HDFS_FOLDER = "/data/peru"
PERU_URL = 'http://www.aduanet.gob.pe/aduanas/informae/presentacion_bases_web.htm'
BASE_URL = 'http://www.aduanet.gob.pe'
TEMP_FOLDER = 'temp/'
PERU_SEENFILES_PATH = 'peru.json'


def download_file(full_link, noext_filepath):
    print("downloading " + full_link)
    response = requests.get(full_link, verify=False)
    with open(noext_filepath + '.zip', 'wb') as file:
        file.write(response.content)


def extract_zip(noext_filepath):
    zip_filepath = noext_filepath + '.zip'
    extract_archive(zip_filepath, outdir=TEMP_FOLDER, verbosity=-1)
    os.remove(zip_filepath)


def convert_dbf_to_csv(noext_filepath):
    table = DBF(noext_filepath + '.DBF')
    csv_filepath = noext_filepath + '.csv'
    with open(csv_filepath, 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(table.field_names)
        for record in table:
            writer.writerow(list(record.values()))
    os.remove(noext_filepath + '.DBF')


def upload_csv_to_hdfs(hdfs_client, noext_filepath):
    csv_filepath = noext_filepath + '.csv'
    hdfs_client.upload(os.path.join(PERU_HDFS_FOLDER, os.path.basename(csv_filepath)), csv_filepath)
    os.remove(noext_filepath + '.csv')


def main():
    hdfs_client = InsecureClient(HDFS_SERVER, user=HDFS_USERNAME)
    seen_files = load_seen_files(PERU_SEENFILES_PATH)
    links = get_links_at(PERU_URL)

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    for link in links:
        link = link.replace('\\', '/')
        filename = os.path.basename(link)
        if filename in seen_files:
            continue
        if not filename.endswith('.zip'):
            continue
        # if not filename.startswith(('x', 'ma', 'mb')): TODO: Fix issue with imports files
        if not filename.startswith('x'):
            continue
        seen_files.append(filename)

        noext_filepath = f'{TEMP_FOLDER}{os.path.splitext(filename)[0]}'
        download_file(f'{BASE_URL}{link}', noext_filepath)
        extract_zip(noext_filepath)
        convert_dbf_to_csv(noext_filepath)
        upload_csv_to_hdfs(hdfs_client, noext_filepath)
    os.rmdir(TEMP_FOLDER)

    save_seen_files(PERU_SEENFILES_PATH, seen_files)


if __name__ == '__main__':
    main()
