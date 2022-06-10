import csv
import os
from copy import deepcopy
from multiprocessing import Pool
from typing import Callable, Dict

from happybase import Connection, Table
from hdfs import Client, InsecureClient

from collectors.env import HDFS_SERVER, HBASE_SERVER  # TODO fix path


class Country:

    def __init__(self, name: str, key_generator: Callable[[str, Dict, int], str], csv_delimiter: str = ',',
                 batch_size: int = 1000, base_path: str = None, table_name: str = None) -> None:
        self.name = name
        self.base_path = base_path or f'/data/{name.lower()}'
        self.table_name = table_name or name.lower()
        self.batch_size = batch_size
        self.key_generator = key_generator
        self.csv_delimiter = csv_delimiter


def load_file_to_hbase_new_clients(file: str, country: Country):
    hdfs_client = InsecureClient(HDFS_SERVER, user='bdm')
    hbase_client = Connection(HBASE_SERVER)
    load_file_to_hbase(file, hdfs_client, hbase_client.table(country.table_name), country)


def load_file_to_hbase(file: str, hdfs_client: Client, hbase_client: Table, country: Country):
    print(f'Loading {file}')
    filename = os.path.splitext(os.path.basename(file))
    batch = hbase_client.batch()
    print(f'\tRetrieving {file} from HDFS')
    with hdfs_client.read(f'{country.base_path}/{file}', encoding='utf8') as source:
        reader = csv.reader(source, delimiter=country.csv_delimiter)
        print(f'\tStart loading {filename} to HBase (in batches of {country.batch_size} rows)')
        headers = next(reader)
        for i, row in enumerate(reader, 1):
            data = {f'values:{key}': val for key, val in zip(headers, row)}
            data['values:filename'] = file
            key = country.key_generator(filename[0], data, i)
            batch.put(key, data)
            if i % country.batch_size == 0:
                batch.send()
                batch = hbase_client.batch()
                print(f'\t\tBatch {i // country.batch_size} sent ({filename[0]})')
        batch.send()
        print(f'\t\tLast batch sent')
    print(f'\tFinish loading {filename} to HBase -> Removing file from HDFS')
    hdfs_client.delete(f'{country.base_path}/{file}')
    print('\tFile deleted')


def load_country(hdfs_client: Client, hbase_client: Connection, country: Country):
    files = hdfs_client.list(country.base_path)
    print(f'Loading {country.name} files:\n\t' + '\n\t'.join(files))
    with Pool() as pool:
        iterable = zip(files, (deepcopy(country) for _ in files))
        pool.starmap(load_file_to_hbase_new_clients, iterable, chunksize=2)
        # for file in files:
        #     load_file_to_hbase(file, hdfs_client, hbase_client.table(country.table_name), country)


def peru_key_gen(filename: str, row: Dict, row_number: int) -> str:
    return f'{filename}-{row_number:012d}'


# TODO: Fix chile and brazil key generation
def chile_key_gen(filename: str, row: Dict, row_number: int) -> str:
    return f'{filename}-{row_number:012d}'


def brazil_key_gen(filename: str, row: Dict, row_number: int) -> str:
    return f'{filename}-{row_number:012d}'


def main():
    hdfs_client = InsecureClient(HDFS_SERVER, user='bdm')
    hbase_client = Connection(HBASE_SERVER)
    countries = [
        Country('Peru', peru_key_gen),
        # Country('Chile', chile_key_gen),
        # Country('Brazil', brazil_key_gen, csv_delimiter=';', batch_size=10000),
    ]
    for country in countries:
        load_country(hdfs_client, hbase_client, country)


if __name__ == '__main__':
    main()
