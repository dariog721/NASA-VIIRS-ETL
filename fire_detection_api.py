from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime,timedelta
import time

api1 = 'Your Api Here'
def _get_api():
    try:
        url = 'https://firms.modaps.eosdis.nasa.gov/api/country/csv/api1/VIIRS_NOAA20_NRT/MEX/1'
        df = pd.read_csv(url)
        df['data_time'] = pd.to_datetime(df['acq_date']) + pd.to_timedelta(df['acq_time'], unit='m')
        df['data_insert'] = pd.to_datetime('today')
        return df
    except pd.errors.EmptyDataError:
        print("The data returned by the API is empty.")
        return None
    except pd.errors.ParserError as pe:
        print(f"Error parsing CSV: {pe}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def _load(tbl, if_exist):
    df = _get_api()
    conn = BaseHook.get_connection('postgresown')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    try:
        with engine.connect() as connection:
            df.to_sql(f'{tbl}', connection, if_exists = f'{if_exist}', index=False, chunksize=1000)
            current_time = time.ctime()
            print(f"Data imported successful on {current_time}")
    except Exception as e:
        print("Data load error: " + str(e))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('nasa_viirs',  default_args=default_args, 
        schedule_interval='59 05 * * *', catchup=False,) as dag:
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgresown',
        sql='''
            CREATE TABLE IF NOT EXISTS firedetection(
            id SERIAL UNIQUE PRIMARY KEY,
            country_id VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,	
            scan FLOAT,	
            track FLOAT,
            acq_date VARCHAR(50),
            acq_time VARCHAR(50),
            data_time TIMESTAMP,
            data_insert TIMESTAMP,
            satellite VARCHAR(50),
            instrument VARCHAR(50),
            confidence VARCHAR(50),
            version VARCHAR,
            brightness FLOAT,
            bright_t31 FLOAT,
            bright_ti4 FLOAT,
            bright_ti5 FLOAT,
            frp FLOAT,
            daynight VARCHAR(10)
            );
        '''
    )
 
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='availabiliti_id',
        endpoint='/country/csv/api1/VIIRS_NOAA20_NRT/MEX/1'
    )


    load_data =  PythonOperator(
        task_id='load_data',
        python_callable=_load,
        op_args = ['firedetection','append']
        
    )

    create_table >> is_api_available >> load_data
