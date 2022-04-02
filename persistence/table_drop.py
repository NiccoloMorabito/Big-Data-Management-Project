import happybase


def drop_table(table_name):
    connection = happybase.Connection('victreebel.fib.upc.edu')
    print(f'Dropping table {table_name} ... ', end='')
    connection.delete_table(table_name, True)
    print('Dropped')


def drop_all_tables():
    connection = happybase.Connection('victreebel.fib.upc.edu')
    tables = connection.tables()
    print(f'Dropping {len(tables)} tables: {", ".join(tables)}')
    for table in tables:
        print(f'\tDropping table {table} ... ', end='')
        connection.delete_table(table, True)
        print('Dropped')


if __name__ == '__main__':
    drop_table('peru')
