import happybase

from collectors.env import HBASE_SERVER


def main():
    connection = happybase.Connection(HBASE_SERVER)
    connection.create_table('peru', {'values': {}})
    connection.create_table('brazil', {'values': {}})
    connection.create_table('chile', {'values': {}})
    print(connection.tables())


if __name__ == '__main__':
    main()
