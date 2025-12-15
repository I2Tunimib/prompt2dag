from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

def fetch_data_from_api(url, **kwargs):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def download_csv_data(url, file_path, **kwargs):
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, 'wb') as f:
        f.write(response.content)

def load_data_to_redis(file_path, redis_key, **kwargs):
    redis_hook = RedisHook(redis_conn_id='redis_default')
    with open(file_path, 'r') as f:
        data = f.read()
    redis_hook.set(redis_key, data)

def clean_thermal_plant_data(file_path, output_path, **kwargs):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'old_column': 'new_column'})
    df = df[df['some_column'] > 0]
    df.to_csv(output_path, index=False)

def clean_nuclear_plant_data(file_path, output_path, **kwargs):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'old_column': 'new_column'})
    df = df[df['some_column'] > 0]
    df.to_csv(output_path, index=False)

def check_death_data_exists(file_path, **kwargs):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        return 'cleanse_death_records'
    else:
        return 'skip_death_data_processing'

def cleanse_death_records(file_path, output_path, **kwargs):
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    df['city'] = df['city'].apply(lambda x: x.title())
    df.to_csv(output_path, index=False)

def store_data_in_postgres(file_path, table_name, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(file_path)
    df.to_sql(table_name, postgres_hook.get_sqlalchemy_engine(), if_exists='append', index=False)

def clean_temp_files(file_path, **kwargs):
    if os.path.exists(file_path):
        os.remove(file_path)

def generate_sql_queries(file_path, output_path, **kwargs):
    df = pd.read_csv(file_path)
    sql_queries = [f"INSERT INTO power_plants (column1, column2) VALUES ('{row['column1']}', '{row['column2']}');" for _, row in df.iterrows()]
    with open(output_path, 'w') as f:
        f.write('\n'.join(sql_queries))

with DAG(
    'french_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for French government death records and power plant data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup('data_ingestion') as data_ingestion:
        fetch_nuclear_metadata = SimpleHttpOperator(
            task_id='fetch_nuclear_metadata',
            http_conn_id='data_gouv_fr',
            endpoint='nuclear_power_plants',
            method='GET',
            response_filter=lambda response: response.json(),
            log_response=True,
        )

        fetch_thermal_metadata = SimpleHttpOperator(
            task_id='fetch_thermal_metadata',
            http_conn_id='data_gouv_fr',
            endpoint='thermal_power_plants',
            method='GET',
            response_filter=lambda response: response.json(),
            log_response=True,
        )

        fetch_death_records = SimpleHttpOperator(
            task_id='fetch_death_records',
            http_conn_id='data_gouv_fr',
            endpoint='death_records',
            method='GET',
            response_filter=lambda response: response.json(),
            log_response=True,
        )

        fetch_city_coordinates = SimpleHttpOperator(
            task_id='fetch_city_coordinates',
            http_conn_id='data_gouv_fr',
            endpoint='city_coordinates',
            method='GET',
            response_filter=lambda response: response.json(),
            log_response=True,
        )

    with TaskGroup('data_processing') as data_processing:
        download_nuclear_data = PythonOperator(
            task_id='download_nuclear_data',
            python_callable=download_csv_data,
            op_kwargs={'url': 'http://data.gouv.fr/nuclear_data.csv', 'file_path': '/tmp/nuclear_data.csv'},
        )

        download_thermal_data = PythonOperator(
            task_id='download_thermal_data',
            python_callable=download_csv_data,
            op_kwargs={'url': 'http://data.gouv.fr/thermal_data.csv', 'file_path': '/tmp/thermal_data.csv'},
        )

        download_death_records = PythonOperator(
            task_id='download_death_records',
            python_callable=download_csv_data,
            op_kwargs={'url': 'http://data.gouv.fr/death_records.csv', 'file_path': '/tmp/death_records.csv'},
        )

    create_deaths_table = PostgresOperator(
        task_id='create_deaths_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS deaths (
            id SERIAL PRIMARY KEY,
            date DATE,
            city VARCHAR(255),
            count INTEGER
        );
        """,
    )

    create_power_plants_table = PostgresOperator(
        task_id='create_power_plants_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS power_plants (
            id SERIAL PRIMARY KEY,
            plant_type VARCHAR(255),
            location VARCHAR(255),
            capacity INTEGER
        );
        """,
    )

    with TaskGroup('data_loading') as data_loading:
        load_death_records_to_redis = PythonOperator(
            task_id='load_death_records_to_redis',
            python_callable=load_data_to_redis,
            op_kwargs={'file_path': '/tmp/death_records.csv', 'redis_key': 'death_records'},
        )

        clean_thermal_data = PythonOperator(
            task_id='clean_thermal_data',
            python_callable=clean_thermal_plant_data,
            op_kwargs={'file_path': '/tmp/thermal_data.csv', 'output_path': '/tmp/clean_thermal_data.csv'},
        )

        clean_nuclear_data = PythonOperator(
            task_id='clean_nuclear_data',
            python_callable=clean_nuclear_plant_data,
            op_kwargs={'file_path': '/tmp/nuclear_data.csv', 'output_path': '/tmp/clean_nuclear_data.csv'},
        )

    branch_death_data = BranchPythonOperator(
        task_id='branch_death_data',
        python_callable=check_death_data_exists,
        op_kwargs={'file_path': '/tmp/death_records.csv'},
    )

    skip_death_data_processing = DummyOperator(task_id='skip_death_data_processing')

    with TaskGroup('death_data_processing') as death_data_processing:
        cleanse_death_records = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=cleanse_death_records,
            op_kwargs={'file_path': '/tmp/death_records.csv', 'output_path': '/tmp/clean_death_records.csv'},
        )

        store_death_records = PythonOperator(
            task_id='store_death_records',
            python_callable=store_data_in_postgres,
            op_kwargs={'file_path': '/tmp/clean_death_records.csv', 'table_name': 'deaths'},
        )

        clean_temp_death_files = PythonOperator(
            task_id='clean_temp_death_files',
            python_callable=clean_temp_files,
            op_kwargs={'file_path': '/tmp/death_records.csv'},
        )

    with TaskGroup('power_plant_data_processing') as power_plant_data_processing:
        generate_sql_queries = PythonOperator(
            task_id='generate_sql_queries',
            python_callable=generate_sql_queries,
            op_kwargs={'file_path': '/tmp/clean_thermal_data.csv', 'output_path': '/tmp/thermal_sql_queries.sql'},
        )

        store_power_plants = PythonOperator(
            task_id='store_power_plants',
            python_callable=store_data_in_postgres,
            op_kwargs={'file_path': '/tmp/clean_thermal_data.csv', 'table_name': 'power_plants'},
        )

    complete_staging = DummyOperator(task_id='complete_staging')

    start_pipeline >> data_ingestion
    data_ingestion >> data_processing
    data_processing >> [create_deaths_table, create_power_plants_table]
    [create_deaths_table, create_power_plants_table] >> data_loading
    data_loading >> branch_death_data
    branch_death_data >> [skip_death_data_processing, death_data_processing]
    death_data_processing >> complete_staging
    skip_death_data_processing >> complete_staging
    complete_staging >> power_plant_data_processing