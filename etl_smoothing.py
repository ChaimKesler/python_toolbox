# Anthony Kesler
# 2017-06-29
# Data ETL: Get, Standardize, Post

# This is an untested template script
# THIS WILL DROP TABLES
# Do not use in Prod without testing in lower environments 

# General Libraries
import sys
import argparse

# SQL Libraries
import pymysql
from sqlalchemy import create_engine

# Math and Data Libraries
import numpy as np
import pandas as pd
import pandas.io.sql as sql
from sklearn.preprocessing import StandardScaler


def etl_data(db_data, pull_table, target_table, chunk_size):
    standardize = StandardScaler()
    offset = 0  # For counting the chunk
    chunk = chunk_size  # Set chunk variable

    # Connect to database for count of rows in table
    connection = pymysql.connect(**db_data)

    schema = db_data['db']

    # Connect to database for sqlalchemy -- mysql as a generic example
    engine_string = ("""mysql+pymysql://{}:{}@{}:{}/{}""").format(db_data['user'],db_data['password'],db_data['host'],db_data['port'],db_data['db'])

    with connection.cursor() as cursor:
        cursor.execute(("""SELECT COUNT(1) FROM {}.{}""").format(schema, pull_table))
        count = cursor.fetchone()
        row_count = count['COUNT(1)']
        print("Starting smoothing of " + str(row_count))
        try:
            print('Starting chunks...')
            while offset < row_count:
            raw = sql.read_sql_query(("""SELECT * FROM {}.{} LIMIT {} OFFSET{}""").format(schema, pull_table, chunk, offset))
            data = data.fillna(data.median())  # Fill NaN to avoid analysis issues - Median as example
            data.loc[:, data.dtypes != object] = standardize.fit_transform(data.loc[:, data.dtypes != object])
            try:
                engine = create_engine(engine_string, echo=False)
                # Warning: 'replace' will drop and recreate the table - read to_sql documentation
                data.to_sql(name = target_table, con = engine, if_exists = 'replace', index = False)
            except Exception as ex:
                print(ex)
            offset += chunk
            print("Up to " + str(offset) + "\tchunked rows transformed.")
            if offset >= row_count:
                print("Done:\n Offset: " + str(offset) + "\nRow Count: " + str(row_count))
                break
        except Exception as ex:
            print(ex)
        connection.close()

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--preference_path', help='Filepath to database settings')
    parser.add_argument('-c', '--chunk_size', help='Size of chunks for pulling data')
    parser.add_argument('-t', '--transform_table', help = 'table of data being transformed')
    parser.add_argument('-l', '--load_table', help='table where data is being loaded')

    args = parser.parse_args()

    preferences = args.preference_path
    chunk = args.chunk_size
    transform = args.transform_table
    load = args.load_table

    try:
        print("Loading database preferences...")
        sys.path.append(preferences)  # Filepath to database settings
        from db_conn import db_data  # Connect to database
            try:
                print('Starting pull of raw data...')
                etl_data(db_data, transform, load, chunk)  # Data cleaning function
             except Exception as ex:
                print(ex)
    except Exception as ex::
        print(ex)

if __name__ == "__main__":
    main()

