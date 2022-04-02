import csv
import os
from typing import List

from tqdm import tqdm
from happybase import Connection, Table
from hdfs import Client, InsecureClient

BASE_PATH = '/data/peru/temp'


def get_files_from_hdfs(hdfs_client: Client) -> List[str]:
    return hdfs_client.list(BASE_PATH)


def load_file_to_hbase(file: str, hdfs_client: Client, hbase_client: Table, batch_size=5000):
    print(f'Loading {file}')
    filename = os.path.splitext(os.path.basename(file))
    batch = hbase_client.batch()
    print(f'\tRetrieving {file} from HDFS')
    with hdfs_client.read(f'{BASE_PATH}/{file}', encoding='utf8') as source:
        reader = csv.reader(source)
        print(f'\tStart loading data to HBase (in batches of {batch_size} rows)')
        headers = next(reader)
        for i, row in tqdm(enumerate(reader, 1)):
            batch.put(f'{filename}-{i}', {f'values:{key}': val for key, val in zip(headers, row)})
            if i % batch_size == 0:
                batch.send()
                batch = hbase_client.batch()
                print(f'\t\tBatch {i // batch_size} sent')
        batch.send()
        print(f'\t\tLast batch sent')
    print('\tFinish load to HBase -> Removing file from HDFS')
    hdfs_client.delete(f'{BASE_PATH}/{file}')
    print('\tFile deleted')


def main():
    hdfs_client = InsecureClient('http://tentacool.fib.upc.edu:9870', user='bdm')
    hbase_client = Connection('victreebel.fib.upc.edu')
    files = get_files_from_hdfs(hdfs_client)
    print('Loading following files:\n\t' + '\n\t'.join(files))
    for file in files:
        load_file_to_hbase(file, hdfs_client, hbase_client.table('peru'))


if __name__ == '__main__':
    main()
