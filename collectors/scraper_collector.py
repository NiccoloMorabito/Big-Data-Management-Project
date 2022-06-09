import os
from json import JSONDecodeError
from typing import List

import requests
from bs4 import BeautifulSoup
from hdfs import InsecureClient, HdfsError
from patoolib import extract_archive

from collectors.utils import basename_without_extension, join_path
from env import HDFS_SERVER, HDFS_USERNAME


class ScraperCollector:
    def __init__(self, links_url: str, base_link_url: str, hdfs_name: str, verbose=True,
                 temp_folder: str = './temp') -> None:
        super().__init__()
        self.links_url = links_url
        self.base_link_url = base_link_url
        self.hdfs_name = hdfs_name
        self.seen_files_path = f'/data/seen_files/{self.hdfs_name}.txt'
        self.hdfs_base_folder = f'/data/{self.hdfs_name}'
        self.verbose = verbose
        self.temp_folder = temp_folder

        self.hdfs_client = InsecureClient(HDFS_SERVER, user=HDFS_USERNAME)
        self.seen_files = self.load_seen_files()

        if not os.path.exists(self.temp_folder):
            os.mkdir(self.temp_folder)

    def get_links(self, filter_extension: str = '') -> List[str]:
        if self.verbose:
            print(f'Getting links from {self.links_url}', end='')
        req = requests.get(self.links_url).text
        soup = BeautifulSoup(req, features='lxml')
        anchors = soup.findAll("a")
        links = [self.base_link_url + link.replace('\\', '/') for link in
                 (link.get("href") for link in anchors) if link and link.endswith(filter_extension)]
        print(f' => {len(links)} links found')
        return links

    def get_unseen_links(self, filter_extension: str = '') -> List[str]:
        links = self.get_links(filter_extension)
        unseen_links = [link for link in links if not self.has_been_seen(link)]
        if self.verbose:
            print(f'\t{len(links) - len(unseen_links)} links already seen => {len(unseen_links)} new links')
        return unseen_links

    def extract_zip(self, file_path: str, remove_zip=True):
        if self.verbose:
            print(f'Extracting zip {file_path}', end='')
        extract_archive(file_path, outdir=self.temp_folder, verbosity=-1)
        if self.verbose:
            print(f' => extracted in {self.temp_folder}')
        if remove_zip:
            if self.verbose:
                print('\tRemoving zip')
            os.remove(file_path)

    def download_file(self, link: str) -> str:
        if self.verbose:
            print(f'Downloading {link}', end='')
        response = requests.get(link, verify=False)
        file_name = os.path.basename(link)
        file_path = join_path(self.temp_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        if self.verbose:
            print(f' => saved in {file_path}')
        return file_path

    def upload_to_hdfs(self, file_path: str, remove=True):
        file_name = os.path.basename(file_path)
        hdfs_file_name = join_path(self.hdfs_base_folder, file_name)
        if self.verbose:
            print(f'Uploading {file_path} to HDFS as {hdfs_file_name}', end='')
        self.hdfs_client.upload(hdfs_file_name, file_path)
        if self.verbose:
            print(f' => uploaded and added to seen files')
        self.add_seen_file(basename_without_extension(file_name))
        if remove:
            if self.verbose:
                print(f'Removing local file')
            os.remove(file_path)

    def load_seen_files(self) -> List[str]:
        if self.verbose:
            print(f'Loading previously seen files from {self.seen_files_path}')
        try:
            with self.hdfs_client.read(self.seen_files_path, encoding='utf8') as file:
                return file.read().splitlines()
        except (HdfsError, JSONDecodeError):
            print('Error when loading previously seen files, returning empty list')
            return list()

    def add_seen_file(self, file_name: str):
        if self.verbose:
            print(f'Adding {file_name} to list of seen files')
        self.seen_files.append(basename_without_extension(file_name))
        self.hdfs_client.write(self.seen_files_path, '\n'.join(self.seen_files), overwrite=True)

    def has_been_seen(self, file_name: str) -> bool:
        return basename_without_extension(file_name) in self.seen_files
