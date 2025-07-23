import pandas as pd
import numpy as np
import os 
from sqlalchemy import create_engine
import time
import logging
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s-%(levelname)s - %(message)s",
    filemode = "a"
    # a is for append, it will append the data every time

)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This code will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists ='replace', index=False)
    
def load_raw_data():
    '''This code will load the CSV as dataframe and import into db'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/' + file)
            logging.info(f"Ingesting {file} in db")
            ingest_db(df,file[:-4], engine)
             # the upper code should be always run after running the next code, then add this code and run again.
    end = time.time()
    total_time = (end - start)/60
    # 60 is taken because it is in sec
    logging.info("Ingestion Complete")
    logging.info(f"\nTotal Time Taken: {total_time} minutes")

if __name__ == '__main__':
    load_raw_data
