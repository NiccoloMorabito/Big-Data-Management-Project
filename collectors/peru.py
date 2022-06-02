import csv
import os.path

from dbfread import DBF

from collectors.scraper_collector import ScraperCollector


def convert_dbf_to_csv(file: str) -> str:
    print(f'Converting {file} to csv', end='')
    table = DBF(file, encoding='latin-1')
    file_name = os.path.splitext(file)[0]
    csv_filepath = file_name + '.csv'
    with open(csv_filepath, 'w+', newline='', encoding='utf8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(table.field_names)
        for record in table:
            writer.writerow([x.decode('latin-1').encode('utf8') if x is str else x for x in record.values()])
    os.remove(file)
    print(' => converted')
    return csv_filepath


def main():
    collector = ScraperCollector('http://www.aduanet.gob.pe/aduanas/informae/presentacion_bases_web.htm',
                                 'http://www.aduanet.gob.pe',
                                 'peru')

    links = collector.get_unseen_links(".zip")
    for link in links:
        filename = os.path.basename(link)
        if not filename.startswith(('x', 'ma', 'mb')):
            continue
        file = collector.download_file(link)
        collector.extract_zip(file)
        dbf_file = os.path.splitext(file)[0] + '.DBF'
        converted = convert_dbf_to_csv(dbf_file)
        collector.upload_to_hdfs(converted)


if __name__ == '__main__':
    main()
