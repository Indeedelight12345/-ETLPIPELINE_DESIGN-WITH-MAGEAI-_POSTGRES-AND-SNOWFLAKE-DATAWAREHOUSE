#!/usr/bin/env pytho


import pandas as pd 
import requests
from sqlalchemy import create_engine
import psycopg2
def get_coin_market_price():
    api_key ='ELOR21XRXEUA2XJT'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={api_key}'
    response= requests.get(url)
    response = response.json()
    return response
data = get_coin_market_price()
data

def convert_dataframe(data):
    time_series_data = data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series_data, orient='index')
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df = df.apply(pd.to_numeric, errors='ignore')
    df.index = pd.to_datetime(df.index)
    df=df.reset_index().rename(columns={'index':'Date'})
    return df

df =convert_dataframe(data)
df

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi_design_database')
engine.connect()

from sqlalchemy import create_engine

def create_connection_database():
    try:
        # Corrected URL format: postgresql://username:password@host:port/database
        engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi_design_database')
        engine.connect()
        print('Connection successful')
    except Exception as e:
        print(f'Error in connection: {e}')

#create table bitcoin_data_api
print(pd.io.sql.get_schema(df, name='bitcoin_data_api', con=engine))


df.head(0).to_sql('bitcoin_data_api', con=engine, if_exists='replace')
df.to_csv('bitcoin_data', index=False)


def create_connection_database():
    try:
        engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi_design_database')
        engine.connect()
        print('Connection successful')
        return engine
    except Exception as e:
        print(f'Error in connection: {e}')
        return None

def load_data_in_batches(file_path, table_name, batch_size=20):
    engine = create_connection_database()
    if engine is None:
        return
    try:
        for chunk in pd.read_csv(file_path, chunksize=batch_size):
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f'Loaded a batch of {len(chunk)} records')
        
        print("Data loaded successfully in batches.")
    except Exception as e:
        print(f'Error in loading data: {e}')
    finally:
        engine.dispose()
file_path = 'bitcoin_data'
table_name = 'bitcoin_data_api'
load_data_in_batches(file_path, table_name)






