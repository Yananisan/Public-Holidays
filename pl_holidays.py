import requests
import json
import boto3
import csv
import pandas as pd
from datetime import datetime
from io import StringIO
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator

config_obs            = Variable.get("obs_bucket_variables", deserialize_json = True)
config_obs_keys       = Variable.get("obs_bucket_keys",      deserialize_json = True)
CountryCodes          = ['RU', 'BY', 'UA']

obs_endpoint_url      = config_obs["obs_endpoint_url"]
obs_bucket_target     = "yi-otus-holidays"
obs_secret_key_id     = config_obs_keys["aws_access_key_id"]
obs_secret_access_key = config_obs_keys["aws_secret_access_key"]

Year                  = 2023

session = boto3.session.Session()

s3 = session.client(
    service_name          = 's3',
    endpoint_url          = obs_endpoint_url,
    aws_access_key_id     = obs_secret_key_id,
    aws_secret_access_key = obs_secret_access_key)


def load_to_bucket(csv_buffer, CountryCode):
    s3.put_object(Bucket=obs_bucket_target,
                    Key='{}/{}.csv'.format(Year, CountryCode),
                    Body=csv_buffer.getvalue(),
                    StorageClass='COLD')

def getHolidays(CountryCode):
    print(CountryCode)
    Year = 2023
    request = requests.get(f'https://date.nager.at/api/v3/PublicHolidays/{Year}/{CountryCode}')
    my_json = request.content.decode('utf-8')
    data = json.loads(my_json)
    df = pd.json_normalize(data)
    df.iloc[1:]
    print(df)
    csv_buffer  = StringIO()
    df.to_csv(csv_buffer, sep='|', mode='w', index=False, quoting=csv.QUOTE_NONE)
    load_to_bucket(csv_buffer, CountryCode)

def toDB(CountryCode):
    obj = s3.get_object(Bucket=obs_bucket_target,
                                         Key='{}/{}.csv'.format(Year, CountryCode))
    body = obj['Body']
    csv_buffer = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_buffer), sep='|',  encoding = 'utf-8')
    print(df)

    pg_conn = Variable.get("pg_conn")
    engine = create_engine(f'{pg_conn}')

    df.to_sql('holidays', engine, schema='public', if_exists='append', index=False)
    
with DAG(
    dag_id = 'pl_holidays',
    description = 'DAG for public holidays of different countries',
    schedule_interval = '@daily',
    start_date = datetime(2023,12,1),
    max_active_runs = 1,
    tags = ['Holidays']
) as dag:

    task_start = BashOperator(
        task_id = 'start',
        bash_command='date'
    )

    getting_data_is_ready = DummyOperator(
        task_id = 'getting_data_is_ready',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    for i in range(1, 4):
        @task(task_id='load_source_api_{}'.format(i))
        def processing(p_load_group):
            return getHolidays(CountryCodes[p_load_group-1])
        
        task_start >> processing(i) >> getting_data_is_ready
    
    for i in range(1, 4):
        @task(task_id='load_to_DB_{}'.format(i))
        def loading(p_load_group):
            return toDB(CountryCodes[p_load_group-1])
        
        getting_data_is_ready >> loading(i)