# codingchallenge_audiencedata
This coding challenge uses three audience-related datasets (not provided) that have been gzipped.  

This repository serves two functions:  
1. Unpack the data and insert them into tables in a relational database  
2. Do analyses on the data  

I chose to preprocess the data in python3 and store the data in a postgres database across three tables, one for each dataset.  

## Requirements
This pipeline requires python3 and postgres  

**The following python packages are required for audiencedata_todb.py**:  
configparser  
os  
gzip  
csv  
time  

**The following python package is required for createdb.py**:  
psycopg2  

**The following python package is required for createtbl.py**:  
psycopg2 
 
**The following python package is required for insertrows.py**:  
psycopg2    


## Config
You will need to create a .ini file as a config with the following sections:

**[data]**  
mainpath=path for repo  
datapath=full path for data folder  

**[postgres_connection]**  
defaultdb=the default database  
port=the port for postgres  
user=postgres user name  
password=postgres password  
host=host address  

**[createdb]**  
dbname=name of the database  
createdb=TRUE to create database (anything else is false)  

**[createtbl]**  
createtbls=TRUE to create tables (anything else is false)  
tblnames=comma-separated list of names for each table  
columns=list of columns for each table, where the columns are comma-separated and the tables are semicolon separated e.g. col1,col2,col3;col1,col2,col3  
coltypes=column type corresponding to columns.
   
**configpath.py**  
A python script that only contains a string with the path for the .ini file. My .ini file is in the main directory but it should be flexible.

example:  
'your preferred python shebang'  
configpath = "path/configname.ini"  

## Main Preprocessing Script (audiencedata_todb.py)  
This script will create the postgres database and tables if set to do so in the configuration. It will also read the data from the gzipped datasets and insert them into the tables row by row. It calls several modules.  

## Create database (createdb.py)  
Called by audiencedata_todb.py if the pipeline is configured to do so. Connects to the default postgres database using the configurations written into the .ini file, drops the database if it already exists, and creates the new database. It connects to postgres using psycopg2.  

## Create tables (createtbl.py)  
Called by audiencedata_todb.py if the pipeline is configured to do so. Connects to the database using the configurations written into the .ini file, drops the tables if they already exist, and creates the new table. First it creates a dummy column, then inserts the columns from the configuration, then deletes the dummy column. It connects to postgres using psycopg2.  

## Insert rows into postgres (insertrows.py)  
Called by audiencedata_todb.py. Creates an open multithread-compatible connection pool to the database and inserts rows of data into a table in batches of 100000, using the configurations written into the .ini file. Apostrophes in data are converted to tildas to be properly inserted into postgres (This actually occurs in audiencedata_todb.py prior to calling insertrows).  

--NOTE: Due to a number of exception cases and limited time, I chose to input all data as TEXT and will post-hoc change data to numeric as needed, since there several cases where the column was mostly numeric but sometimes TEXT-based. 

Reasoning: I chose to read the gzipped rows line-by-line to make the pipeline robust to large datasets that might otherwise take up too much memory. I chose to use an open connection pool so that the loading script would not need to constantly connect to and disconnect from the postgres database, which would be slow. It does not currently multi-thread inserts, but is designed in such a way to support that possibility down the line. I chose to batch the data into units of 100,000 rows to make processing faster, as inserting each row individually would take hours, vs. ~2.5minutes on my (slow) machine in these batches, which only took up a very conservative 1.5mb in memory each. Realistically, the data could be batched in much larger sets to minimize speed / memory tradeoff.  

## Analyses  
In development...  