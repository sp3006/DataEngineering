import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import os
import glob
import pandas as pd


   
def load_staging_tables(cur, conn):
    '''
         Args: cur = cursor connect to Redshift DB
         conn = used to connect to Postgres DB
         Iterate & load using COPY imported tables list
    '''
    

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
         Args: cur = cursor to connect & extract from Redshift DB
         conn = used to connect to Postgres DB
         Iterate & Insert imported tables list
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

       
def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    '''
       Getting all the parameter values from the config file and passing to create connection and cursor objects.

    '''
    host = config.get("CLUSTER","HOST")
    dbname = config.get("CLUSTER","DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = config.get("CLUSTER", "DB_PASSWORD")
    port = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password} port={port}")
    cur = conn.cursor()

    '''
      Invoking Load function to load the json file to staging tables and
      invoking insert table function to insert the data into the facts and dimensions tables
    '''
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
