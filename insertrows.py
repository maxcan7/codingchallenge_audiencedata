#!/usr/bin/env py
from psycopg2 import pool


def insertrows(con_config, db_config, tbl_config, threadpool, rows, d):
    '''
    Insert the rows of data into the postgres tables
    '''
    con = threadpool.getconn()
    con.autocommit = True  # Automatically commit all executions
    cur = con.cursor()
    '''
    The below commented code block was for converting numeric data and
    accounting for exceptions, but ultimately did not have time to fully
    diagnose all exceptions
    '''
    # coltypes = tbl_config['coltypes'].split(';')[d].split(',')  # Column types are semicolon separated by table and comma separated within a table
    # for row in rows:
        # for idx in range(len(row)):
            # if coltypes[idx] == 'INTEGER':  # Check for data that needs to be converted to numeric
                # if not any([x for x in tbl_config['exceptions'] if row[idx] == x]):  # Check for exception cases
                    # row[idx] = float(row[idx])
    rows = [tuple(row) for row in rows]
    rows_in = ','.join(cur.mogrify("%s", (x, )).decode('utf8') for x in rows)
    cur.execute("INSERT INTO %s VALUES " % tbl_config['tblnames'].split(",")[d] + rows_in)

    threadpool.putconn(con)  # Put away pooled connection
    