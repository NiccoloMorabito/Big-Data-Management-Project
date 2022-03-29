import csv
import os

import patoolib
from dbfread import DBF


def main():
    data_folder = './data/peru'
    for file in os.listdir(data_folder):
        patoolib.extract_archive(f'{data_folder}/{file}', outdir=data_folder)
        dbf_file = f'{data_folder}/{os.path.splitext(file)[0]}.DBF'
        table = DBF(dbf_file)
        with open(f'{data_folder}/{os.path.splitext(file)[0]}.csv', 'w+', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(table.field_names)
            for record in table:
                writer.writerow(list(record.values()))
        os.remove(dbf_file)

    # TODO Move file to Persistence Zone


if __name__ == '__main__':
    main()
