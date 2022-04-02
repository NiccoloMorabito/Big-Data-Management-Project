import csv
import os

from happybase import Connection, Table
from hdfs import Client, InsecureClient
from tqdm import tqdm


class Country:

    def __init__(self, name, base_path, table_name) -> None:
        self.name = name
        self.base_path = base_path
        self.table_name = table_name


def load_file_to_hbase(file: str, hdfs_client: Client, hbase_client: Table, country: Country, batch_size=1000):
    print(f'Loading {file}')
    filename = os.path.splitext(os.path.basename(file))
    batch = hbase_client.batch()
    print(f'\tRetrieving {file} from HDFS')
    with hdfs_client.read(f'{country.base_path}/{file}', encoding='utf8') as source:
        reader = csv.reader(source)
        print(f'\tStart loading data to HBase (in batches of {batch_size} rows)')
        headers = next(reader)
        for i, row in tqdm(enumerate(reader, 1)):
            data = {f'values:{key}': val for key, val in zip(headers, row)}
            data['values:filename'] = file
            batch.put(f'{filename}-{i}', data)
            if i % batch_size == 0:
                batch.send()
                batch = hbase_client.batch()
                print(f'\t\tBatch {i // batch_size} sent')
        batch.send()
        print(f'\t\tLast batch sent')
    print('\tFinish load to HBase -> Removing file from HDFS')
    hdfs_client.delete(f'{country.base_path}/{file}')
    print('\tFile deleted')


def load_country(hdfs_client: Client, hbase_client: Connection, country: Country):
    files = hdfs_client.list(country.base_path)
    print(f'Loading {country.name} files:\n\t' + '\n\t'.join(files))
    for file in files:
        load_file_to_hbase(file, hdfs_client, hbase_client.table(country.table_name), country)


def main():
    hdfs_client = InsecureClient('http://tentacool.fib.upc.edu:9870', user='bdm')
    hbase_client = Connection('victreebel.fib.upc.edu')
    countries = [
        Country('Peru', '/data/peru', 'peru'),
        Country('Chile', '/data/chile', 'chile'),  # TODO Review hdfs data path
        Country('Brazil', '/data/brazil', 'brazil'),  # TODO Review hdfs data path
    ]
    for country in countries:
        load_country(hdfs_client, hbase_client, country)


if __name__ == '__main__':
    main()
