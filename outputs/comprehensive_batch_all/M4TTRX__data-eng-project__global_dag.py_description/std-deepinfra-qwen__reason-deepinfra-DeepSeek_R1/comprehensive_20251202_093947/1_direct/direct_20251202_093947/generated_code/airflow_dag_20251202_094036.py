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
import os
import requests
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'max_active_runs': 1,
}

def fetch_nuclear_power_plant_metadata(**kwargs):
    response = requests.get('https://data.gouv.fr/api/1/datasets/nuclear-power-plants')
    return response.json()

def fetch_thermal_power_plant_metadata(**kwargs):
    response = requests.get('https://data.gouv.fr/api/1/datasets/thermal-power-plants')
    return response.json()

def fetch_death_records_resource_list(**kwargs):
    response = requests.get('https://data.gouv.fr/api/1/datasets/death-records')
    return response.json()

def fetch_city_geographic_coordinates_csv(**kwargs):
    response = requests.get('https://data.gouv.fr/api/1/datasets/city-geographic-coordinates')
    return response.content

def download_nuclear_power_plant_csv(metadata, **kwargs):
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    with open('/tmp/nuclear_power_plant.csv', 'wb') as f:
        f.write(response.content)

def download_thermal_power_plant_csv(metadata, **kwargs):
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    with open('/tmp/thermal_power_plant.csv', 'wb') as f:
        f.write(response.content)

def download_death_record_files(resource_list, **kwargs):
    for resource in resource_list['resources']:
        url = resource['url']
        response = requests.get(url)
        with open(f"/tmp/{resource['title']}.csv", 'wb') as f:
            f.write(response.content)

def create_deaths_table(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS deaths (
            id SERIAL PRIMARY KEY,
            date DATE,
            city VARCHAR(255),
            coordinates VARCHAR(255)
        );
    """)

def create_power_plants_table(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS power_plants (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(255),
            location VARCHAR(255)
        );
    """)

def load_death_records_to_redis(**kwargs):
    redis_hook = RedisHook(redis_conn_id='redis_default')
    for file in os.listdir('/tmp'):
        if file.endswith('.csv'):
            df = pd.read_csv(f'/tmp/{file}')
            redis_hook.rpush('death_records', df.to_json(orient='records'))

def clean_thermal_plant_data(**kwargs):
    df = pd.read_csv('/tmp/thermal_power_plant.csv')
    df = df.rename(columns={'plant_name': 'name', 'plant_type': 'type', 'plant_location': 'location'})
    df.to_csv('/tmp/clean_thermal_power_plant.csv', index=False)

def clean_nuclear_plant_data(**kwargs):
    df = pd.read_csv('/tmp/nuclear_power_plant.csv')
    df = df.rename(columns={'plant_name': 'name', 'plant_type': 'type', 'plant_location': 'location'})
    df.to_csv('/tmp/clean_nuclear_power_plant.csv', index=False)

def check_death_data_exists(**kwargs):
    redis_hook = RedisHook(redis_conn_id='redis_default')
    return 'process_death_data' if redis_hook.llen('death_records') > 0 else 'skip_death_data'

def cleanse_death_records(**kwargs):
    redis_hook = RedisHook(redis_conn_id='redis_default')
    records = redis_hook.lrange('death_records', 0, -1)
    df = pd.DataFrame([pd.read_json(record) for record in records])
    df['date'] = pd.to_datetime(df['date']).dt.date
    df.to_csv('/tmp/cleaned_death_records.csv', index=False)

def store_cleansed_death_records(**kwargs):
    df = pd.read_csv('/tmp/cleaned_death_records.csv')
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df.to_sql('deaths', hook.get_sqlalchemy_engine(), if_exists='append', index=False)

def clean_temporary_death_data_files(**kwargs):
    for file in os.listdir('/tmp'):
        if file.endswith('.csv'):
            os.remove(f'/tmp/{file}')

def generate_sql_queries_for_plant_data(**kwargs):
    df_thermal = pd.read_csv('/tmp/clean_thermal_power_plant.csv')
    df_nuclear = pd.read_csv('/tmp/clean_nuclear_power_plant.csv')
    df = pd.concat([df_thermal, df_nuclear])
    sql_queries = df.to_sql('power_plants', if_exists='append', index=False, con=PostgresHook(postgres_conn_id='postgres_default').get_sqlalchemy_engine(), method='multi')

def store_power_plant_records(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        INSERT INTO power_plants (name, type, location)
        SELECT name, type, location
        FROM power_plants_temp
    """)

with DAG(
    'french_government_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup('ingest_data') as ingest_data:
        fetch_nuclear_metadata = PythonOperator(
            task_id='fetch_nuclear_power_plant_metadata',
            python_callable=fetch_nuclear_power_plant_metadata,
        )

        fetch_thermal_metadata = PythonOperator(
            task_id='fetch_thermal_power_plant_metadata',
            python_callable=fetch_thermal_power_plant_metadata,
        )

        fetch_death_records = PythonOperator(
            task_id='fetch_death_records_resource_list',
            python_callable=fetch_death_records_resource_list,
        )

        fetch_city_coordinates = PythonOperator(
            task_id='fetch_city_geographic_coordinates_csv',
            python_callable=fetch_city_geographic_coordinates_csv,
        )

    with TaskGroup('process_metadata') as process_metadata:
        download_nuclear_csv = PythonOperator(
            task_id='download_nuclear_power_plant_csv',
            python_callable=download_nuclear_power_plant_csv,
            op_kwargs={'metadata': '{{ ti.xcom_pull(task_ids="ingest_data.fetch_nuclear_power_plant_metadata") }}'},
        )

        download_thermal_csv = PythonOperator(
            task_id='download_thermal_power_plant_csv',
            python_callable=download_thermal_power_plant_csv,
            op_kwargs={'metadata': '{{ ti.xcom_pull(task_ids="ingest_data.fetch_thermal_power_plant_metadata") }}'},
        )

        download_death_files = PythonOperator(
            task_id='download_death_record_files',
            python_callable=download_death_record_files,
            op_kwargs={'resource_list': '{{ ti.xcom_pull(task_ids="ingest_data.fetch_death_records_resource_list") }}'},
        )

    create_deaths_table_task = PythonOperator(
        task_id='create_deaths_table',
        python_callable=create_deaths_table,
    )

    create_power_plants_table_task = PythonOperator(
        task_id='create_power_plants_table',
        python_callable=create_power_plants_table,
    )

    with TaskGroup('process_data') as process_data:
        load_death_records_to_redis_task = PythonOperator(
            task_id='load_death_records_to_redis',
            python_callable=load_death_records_to_redis,
        )

        clean_thermal_plant_data_task = PythonOperator(
            task_id='clean_thermal_plant_data',
            python_callable=clean_thermal_plant_data,
        )

        clean_nuclear_plant_data_task = PythonOperator(
            task_id='clean_nuclear_plant_data',
            python_callable=clean_nuclear_plant_data,
        )

    check_death_data_exists_task = BranchPythonOperator(
        task_id='check_death_data_exists',
        python_callable=check_death_data_exists,
    )

    process_death_data = DummyOperator(task_id='process_death_data')

    skip_death_data = DummyOperator(task_id='skip_death_data')

    with TaskGroup('death_data_processing') as death_data_processing:
        cleanse_death_records_task = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=cleanse_death_records,
        )

        store_cleansed_death_records_task = PythonOperator(
            task_id='store_cleansed_death_records',
            python_callable=store_cleansed_death_records,
        )

        clean_temporary_death_data_files_task = PythonOperator(
            task_id='clean_temporary_death_data_files',
            python_callable=clean_temporary_death_data_files,
        )

    with TaskGroup('power_plant_data_processing') as power_plant_data_processing:
        generate_sql_queries_for_plant_data_task = PythonOperator(
            task_id='generate_sql_queries_for_plant_data',
            python_callable=generate_sql_queries_for_plant_data,
        )

        store_power_plant_records_task = PythonOperator(
            task_id='store_power_plant_records',
            python_callable=store_power_plant_records,
        )

    complete_staging = DummyOperator(task_id='complete_staging')

    start_pipeline >> ingest_data >> process_metadata >> [create_deaths_table_task, create_power_plants_table_task] >> process_data >> check_death_data_exists_task

    check_death_data_exists_task >> [process_death_data, skip_death_data]

    process_death_data >> death_data_processing >> complete_staging

    skip_death_data >> complete_staging

    process_data >> power_plant_data_processing >> complete_staging