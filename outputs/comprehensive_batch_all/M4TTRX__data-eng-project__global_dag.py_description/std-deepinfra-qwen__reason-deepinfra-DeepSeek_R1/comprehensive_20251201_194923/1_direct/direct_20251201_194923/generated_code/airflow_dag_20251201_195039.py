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

def fetch_nuclear_power_plant_metadata():
    response = requests.get('https://data.gouv.fr/api/1/datasets/nuclear-power-plants')
    with open('/tmp/nuclear_power_plants.json', 'w') as f:
        f.write(response.text)

def fetch_thermal_power_plant_metadata():
    response = requests.get('https://data.gouv.fr/api/1/datasets/thermal-power-plants')
    with open('/tmp/thermal_power_plants.json', 'w') as f:
        f.write(response.text)

def fetch_death_records_resource_list():
    response = requests.get('https://data.gouv.fr/api/1/datasets/death-records')
    with open('/tmp/death_records.json', 'w') as f:
        f.write(response.text)

def fetch_city_geographic_coordinates():
    response = requests.get('https://data.gouv.fr/api/1/datasets/city-geographic-coordinates')
    with open('/tmp/city_coordinates.csv', 'w') as f:
        f.write(response.text)

def download_nuclear_power_plant_csv():
    with open('/tmp/nuclear_power_plants.json', 'r') as f:
        metadata = f.read()
    # Simulate CSV download
    with open('/tmp/nuclear_power_plants.csv', 'w') as f:
        f.write('id,name,location\n1,Nuclear Plant 1,Location 1\n2,Nuclear Plant 2,Location 2')

def download_thermal_power_plant_csv():
    with open('/tmp/thermal_power_plants.json', 'r') as f:
        metadata = f.read()
    # Simulate CSV download
    with open('/tmp/thermal_power_plants.csv', 'w') as f:
        f.write('id,name,location\n1,Thermal Plant 1,Location 1\n2,Thermal Plant 2,Location 2')

def download_death_record_files():
    with open('/tmp/death_records.json', 'r') as f:
        metadata = f.read()
    # Simulate CSV download
    with open('/tmp/death_records.csv', 'w') as f:
        f.write('id,name,date,location\n1,John Doe,2023-01-01,Paris\n2,Jane Doe,2023-01-02,Lyon')

def create_deaths_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("CREATE TABLE IF NOT EXISTS deaths (id SERIAL PRIMARY KEY, name TEXT, date DATE, location TEXT);")

def create_power_plants_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("CREATE TABLE IF NOT EXISTS power_plants (id SERIAL PRIMARY KEY, name TEXT, location TEXT, type TEXT);")

def load_death_records_to_redis():
    df = pd.read_csv('/tmp/death_records.csv')
    redis_hook = RedisHook(redis_conn_id='redis_default')
    redis_hook.rpush('death_records', df.to_json(orient='records'))

def clean_thermal_plant_data():
    df = pd.read_csv('/tmp/thermal_power_plants.csv')
    df = df.rename(columns={'id': 'thermal_id', 'name': 'thermal_name'})
    df.to_csv('/tmp/clean_thermal_power_plants.csv', index=False)

def clean_nuclear_plant_data():
    df = pd.read_csv('/tmp/nuclear_power_plants.csv')
    df = df.rename(columns={'id': 'nuclear_id', 'name': 'nuclear_name'})
    df.to_csv('/tmp/clean_nuclear_power_plants.csv', index=False)

def check_death_data_exists():
    if os.path.exists('/tmp/death_records.csv'):
        return 'cleanse_death_records'
    else:
        return 'skip_death_data_processing'

def cleanse_death_records():
    df = pd.read_csv('/tmp/death_records.csv')
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv('/tmp/cleansed_death_records.csv', index=False)

def store_cleansed_death_records():
    df = pd.read_csv('/tmp/cleansed_death_records.csv')
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df.to_sql('deaths', hook.get_sqlalchemy_engine(), if_exists='append', index=False)

def clean_temporary_death_data_files():
    os.remove('/tmp/death_records.csv')
    os.remove('/tmp/cleansed_death_records.csv')

def generate_sql_queries_for_plant_data():
    with open('/tmp/plant_data_queries.sql', 'w') as f:
        f.write("INSERT INTO power_plants (name, location, type) VALUES ('Nuclear Plant 1', 'Location 1', 'Nuclear');\n")
        f.write("INSERT INTO power_plants (name, location, type) VALUES ('Thermal Plant 1', 'Location 1', 'Thermal');")

def store_power_plant_records():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    with open('/tmp/plant_data_queries.sql', 'r') as f:
        queries = f.read()
    hook.run(queries)

with DAG(
    'french_government_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
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
            task_id='fetch_city_geographic_coordinates',
            python_callable=fetch_city_geographic_coordinates,
        )

    with TaskGroup('process_metadata') as process_metadata:
        download_nuclear_csv = PythonOperator(
            task_id='download_nuclear_power_plant_csv',
            python_callable=download_nuclear_power_plant_csv,
        )

        download_thermal_csv = PythonOperator(
            task_id='download_thermal_power_plant_csv',
            python_callable=download_thermal_power_plant_csv,
        )

        download_death_files = PythonOperator(
            task_id='download_death_record_files',
            python_callable=download_death_record_files,
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

    check_death_data = BranchPythonOperator(
        task_id='check_death_data_exists',
        python_callable=check_death_data_exists,
    )

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

    skip_death_data_processing = DummyOperator(task_id='skip_death_data_processing')

    with TaskGroup('power_plant_data_processing') as power_plant_data_processing:
        generate_sql_queries_task = PythonOperator(
            task_id='generate_sql_queries_for_plant_data',
            python_callable=generate_sql_queries_for_plant_data,
        )

        store_power_plant_records_task = PythonOperator(
            task_id='store_power_plant_records',
            python_callable=store_power_plant_records,
        )

    complete_staging = DummyOperator(task_id='complete_staging')

    start_pipeline >> ingest_data
    ingest_data >> process_metadata
    process_metadata >> [create_deaths_table_task, create_power_plants_table_task]
    create_deaths_table_task >> load_death_records_to_redis_task
    create_power_plants_table_task >> process_data
    process_data >> check_death_data
    check_death_data >> [death_data_processing, skip_death_data_processing]
    death_data_processing >> complete_staging
    skip_death_data_processing >> complete_staging
    process_data >> power_plant_data_processing
    power_plant_data_processing >> complete_staging