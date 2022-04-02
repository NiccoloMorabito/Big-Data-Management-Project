import csv
import os
from typing import List

from happybase import Connection, Table
from hdfs import Client, InsecureClient


def get_files_from_hdfs(hdfs_client: Client) -> List[str]:
    return hdfs_client.list('/data/peru')


def load_file_to_hbase(file: str, hdfs_client: Client, hbase_client: Table, batch_size=1000):
    filename = os.path.splitext(os.path.basename(file))
    batch = hbase_client.batch()
    with hdfs_client.read(file) as source:
        reader = csv.reader(source)
        headers = next(reader)
        for i, row in enumerate(reader, 1):
            batch.put(f'{filename}-{i}', {f'values:{key}': val for key, val in zip(headers, row)})
            if i % batch_size == 0:
                batch.send()
                batch = hbase_client.batch()


def main():
    hdfs_client = InsecureClient('http://127.0.0.1:9870', user='bdm')
    hbase_client = Connection('victreebel.fib.upc.edu')
    files = get_files_from_hdfs(hdfs_client)
    for file in files:
        load_file_to_hbase(file, hdfs_client, hbase_client.table('peru'))


if __name__ == '__main__':
    main()
