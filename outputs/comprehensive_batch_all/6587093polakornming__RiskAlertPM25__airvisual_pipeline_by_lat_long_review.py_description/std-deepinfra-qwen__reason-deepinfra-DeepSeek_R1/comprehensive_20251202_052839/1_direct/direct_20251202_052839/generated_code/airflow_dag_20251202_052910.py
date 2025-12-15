from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
import json
import pytz
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(minutes=60),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='air_quality_etl_pipeline',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='ETL pipeline for air quality data from AirVisual API to PostgreSQL',
) as dag:

    def get_airvisual_data_hourly(**kwargs):
        api_key = 'your_api_key'
        latitude = 13.7563
        longitude = 100.3084
        url = f"https://api.airvisual.com/v2/nearest_city?lat={latitude}&lon={longitude}&key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if 'data' not in data:
            raise ValueError("Invalid API response")

        # Convert timestamp to Bangkok timezone
        timestamp = datetime.fromisoformat(data['data']['time']['iso']).astimezone(pytz.timezone('Asia/Bangkok'))
        data['data']['time']['iso'] = timestamp.isoformat()

        # Check if data already exists in the database
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
        query = f"SELECT * FROM factairvisualtable WHERE timestamp = '{timestamp}'"
        existing_data = postgres_hook.get_records(query)
        if existing_data:
            raise AirflowSkipException("Data already exists in the database")

        # Atomic file write pattern
        temp_file = '/tmp/airvisual_data.json'
        with open(temp_file, 'w') as f:
            json.dump(data, f)

    def read_data_airvisual(**kwargs):
        temp_file = '/tmp/airvisual_data.json'
        if not os.path.exists(temp_file):
            raise FileNotFoundError("Temporary JSON file not found")

        with open(temp_file, 'r') as f:
            data = json.load(f)

        if 'data' not in data:
            raise ValueError("Invalid data structure in JSON file")

        print("Data extraction successful")

    def load_airvisual_to_postgresql(**kwargs):
        temp_file = '/tmp/airvisual_data.json'
        with open(temp_file, 'r') as f:
            data = json.load(f)

        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')

        # Load pollution and weather mapping configurations
        with open('/path/to/pollution_mapping.json', 'r') as f:
            pollution_mapping = json.load(f)
        with open('/path/to/weather_mapping.json', 'r') as f:
            weather_mapping = json.load(f)

        # Insert timestamp data into dimDateTimeTable
        timestamp = data['data']['time']['iso']
        postgres_hook.run(f"INSERT INTO dimDateTimeTable (timestamp) VALUES ('{timestamp}') ON CONFLICT DO NOTHING")

        # Insert location data into dimLocationTable
        location = data['data']['city']
        postgres_hook.run(f"INSERT INTO dimLocationTable (location) VALUES ('{location}') ON CONFLICT DO NOTHING")

        # Insert pollution metadata into dimMainPollutionTable
        pollution_data = data['data']['pollution']
        for key, value in pollution_data.items():
            if key in pollution_mapping:
                postgres_hook.run(f"INSERT INTO dimMainPollutionTable (pollutant, value) VALUES ('{pollution_mapping[key]}', {value}) ON CONFLICT DO NOTHING")

        # Insert main fact data into factairvisualtable
        fact_data = {
            'timestamp': timestamp,
            'location': location,
            'pollution_data': pollution_data,
            'weather_data': data['data']['weather']
        }
        postgres_hook.run(f"INSERT INTO factairvisualtable (timestamp, location, pollution_data, weather_data) VALUES ('{timestamp}', '{location}', '{json.dumps(pollution_data)}', '{json.dumps(data['data']['weather'])}') ON CONFLICT DO NOTHING")

    get_airvisual_data_hourly_task = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_airvisual_data_hourly,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    read_data_airvisual_task = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_data_airvisual,
        provide_context=True,
    )

    load_airvisual_to_postgresql_task = PythonOperator(
        task_id='load_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgresql,
        provide_context=True,
    )

    get_airvisual_data_hourly_task >> read_data_airvisual_task >> load_airvisual_to_postgresql_task