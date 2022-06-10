import happybase

from collectors.env import HBASE_SERVER


def drop_table(table_name):
    connection = happybase.Connection(HBASE_SERVER)
    print(f'Dropping table {table_name} ... ', end='')
    connection.delete_table(table_name, True)
    print('Dropped')


def drop_all_tables():
    connection = happybase.Connection(HBASE_SERVER)
    tables = connection.tables()
    print(f'Dropping {len(tables)} tables: {", ".join(tables)}')
    for table in tables:
        print(f'\tDropping table {table} ... ', end='')
        connection.delete_table(table, True)
        print('Dropped')


if __name__ == '__main__':
    drop_table('peru')
