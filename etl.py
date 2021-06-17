import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, insert_print, copy_print


def load_staging_tables(cur, conn):
    
    """
    
    Description: This function is responsible for loading log and song data from S3, 
    and then, inserting data into staging tables on Redshift. 
 
    Returns:
        None.
    
    """    
    
    for i in range(len(copy_table_queries)):
        
        print('Loading staging table: ', copy_print[i])
        
        cur.execute(copy_table_queries[i])
        conn.commit()


def insert_tables(cur, conn):
    
    """
    
    Description: This function is responsible for extracting data from staging tables. 
    Extracting the timestamp, hour, day, week of year, month, year, 
    and weekday from the ts column for time table. Insert associated data to time 
    and user dimensional tables. Find song ID and artist ID for the records in the log data 
    based on the title, artist name, and duration from song and artist tables. Insert associated data 
    to songplay fact table.
  
    Returns:
        None.
    
    """
    
    for i in range(len(insert_table_queries)):
        
        print('Inserting table: ', insert_print[i])
        
        cur.execute(insert_table_queries[i])
        conn.commit()
        


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
    
