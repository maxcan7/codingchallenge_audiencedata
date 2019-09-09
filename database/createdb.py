#!/usr/bin/env py
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def createdb(con_config, db_config):
    '''
    Create the postgres database
    '''
    con = connect(database=con_config['defaultdb'], port=con_config['port'], 
                  user=con_config['user'], password=con_config['password'], 
                  host=con_config['host'])
    newdb = db_config['dbname']
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS %s' %newdb)
    cur.execute('CREATE DATABASE %s' %newdb)
    cur.close()
