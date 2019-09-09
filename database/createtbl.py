#!/usr/bin/env py
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def createtbl(con_config, db_config, tbl_config):
    '''
    Create the postgres tables
    '''
    con = connect(database=db_config['dbname'], port=con_config['port'], 
                  user=con_config['user'], password=con_config['password'], 
                  host=con_config['host'])
    con.autocommit = True  # Automatically commit all executions
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    for t in range(len(tbl_config['tblnames'].split(','))):  # Create tables
        tblname = tbl_config['tblnames'].split(',')[t]  # Table names are comma separated
        columns = tbl_config['columns'].split(';')[t].split(',')  # Column names are semicolon separated by table and comma separated within a table
        coltypes = tbl_config['coltypes'].split(';')[t].split(',')  # Column types are semicolon separated by table and comma separated within a table
        cur.execute("DROP TABLE IF EXISTS %s" %tblname)  # Drops the table if it already exists
        cur.execute("CREATE TABLE IF NOT EXISTS %s(dummy INTEGER)" %tblname)  # Create an "empty" table (has a dummy column)
        for c in range(len(columns)):  # Add columns and their type for each table
            cur.execute("ALTER TABLE %s ADD COLUMN %s %s;" % (tblname, columns[c], coltypes[c]))
        cur.execute("ALTER TABLE %s DROP COLUMN dummy;" % tblname)  # Drop dummy column
    cur.close()