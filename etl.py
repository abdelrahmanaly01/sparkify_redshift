import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('loading table...')
        print(query[0:40])
        cur.execute(query)
        conn.commit()
        print('one table is done')
    print('done with staging tables')
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('loading table...')
        print(query[0:40])
        cur.execute(query)
        conn.commit()
        print('one table is done')
    print('done with DWH')
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()