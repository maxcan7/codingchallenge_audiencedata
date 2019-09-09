#!/usr/bin/env py
from configpath import configpath
from configparser import ConfigParser
from database.createdb import createdb
from database.createtbl import createtbl
from psycopg2 import pool
from insertrows import insertrows
from os import listdir
import gzip
import csv
from time import time


start = time()  # Track time of pipeline


def load_config(configpath, section):
    '''
    Use configparser to load the .ini file
    '''
    parser = ConfigParser()  # Create parser
    parser.read(configpath)  # Read config ini file
    config = {}  # Create empty dictionary for config
    if parser.has_section(section):  # Look for 'config' section in config ini file
        params = parser.items(section)  # Parse config ini file
        for param in params:  # Loop through parameters
            config[param[0]] = param[1]  # Set key-value pair for parameter in dictionary
    else:  # Raise exception if the section can't be found
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, configpath))
    return config


def load_and_insert(data_config, con_config, db_config, tbl_config):
    '''
    From the configs, collect the paths for the data files and establish a 
    thread pool connection for psycopg2 to insert data into postgres. Loop
    through each data file, then loop through each row of data, and insert
    one by one.
    '''
    datapaths = [data_config['datapath'] + d for d in listdir(data_config['datapath'])]
    threadpool = pool.ThreadedConnectionPool(5, 20, database=db_config['dbname'], port=con_config['port'],
                                             user=con_config['user'], password=con_config['password'],
                                             host=con_config['host'])  # Use pooling to multi-thread connect to db, defaulting 5 threads and up to 20 threads
    for d in range(len(datapaths)):  # Loop through data directory
        f = gzip.open(datapaths[d], "rt", encoding='utf8')  # Open gzipped data file
        reader = csv.reader(f)  # Create a reader
        next(reader)  # Skip headers by iterating over them prior to loop
        rows = []  # Assign empty list for rows
        cnt = 0  # Counter for batching rows
        for row in reader:  # Iterate over all rows
            rows.append(row)
            cnt += 1
            if cnt > 100000:  # Batch in sets of 100,000 rows
                rows = [[x.replace("'", "`") for x in row] for row in rows]  # Replace apostrophes with tildas to insert into postgres table
                insertrows(con_config, db_config, tbl_config, threadpool, rows, d)  # Insert row into postgres
                cnt = 0  # Reset counter
                rows = []  # Reset rows batch list
            elif any(reader) is False:  # Send the rest off if < 100000 remaining
                rows = [[x.replace("'", "`") for x in row] for row in rows]  # Replace apostrophes with tildas to insert into postgres table
                insertrows(con_config, db_config, tbl_config, threadpool, rows, d)  # Insert row into postgres
                cnt = 0  # Reset counter
                rows = []  # Reset rows batch list


if __name__ == '__main__':
    data_config = load_config(configpath, 'data')  # Load data paths
    con_config = load_config(configpath, 'postgres_connection')  # Load postgres connection config
    db_config = load_config(configpath, 'createdb')  # Load database config
    tbl_config = load_config(configpath, 'createtbl')  # Load tables config
    if db_config['createdb'] == 'TRUE':
        createdb(con_config, db_config)   # Configurable, create database
    if tbl_config['createtbls'] == 'TRUE':
        createtbl(con_config, db_config, tbl_config)  # Configurable, create tables
    load_and_insert(data_config, con_config, db_config, tbl_config)  # Load data and insert into postgres

end = time()
