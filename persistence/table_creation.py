import happybase


def main():
    connection = happybase.Connection('victreebel.fib.upc.edu')
    connection.create_table('peru', {'values': {}})
    connection.create_table('brazil', {'values': {}})
    connection.create_table('chile', {'values': {}})
    print(connection.tables())


if __name__ == '__main__':
    main()
