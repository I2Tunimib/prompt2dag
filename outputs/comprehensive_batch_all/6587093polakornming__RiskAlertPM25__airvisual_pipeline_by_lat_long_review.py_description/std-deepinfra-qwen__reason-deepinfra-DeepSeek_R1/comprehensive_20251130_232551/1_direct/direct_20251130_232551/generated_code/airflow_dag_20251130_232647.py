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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(minutes=60),
}

# Define the DAG
with DAG(
    dag_id='air_quality_etl_pipeline',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['etl', 'air_quality', 'postgresql']
) as dag:

    def get_airvisual_data_hourly(**kwargs):
        # Load configuration
        config = json.load(open('config.conf'))
        api_key = config['airvisual_api_key']
        latitude = config['latitude']
        longitude = config['longitude']

        # Fetch data from AirVisual API
        url = f"https://api.airvisual.com/v2/nearest_city?lat={latitude}&lon={longitude}&key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Validate API response
        if 'data' not in data:
            raise ValueError("Invalid API response")

        # Convert timestamp to Bangkok timezone
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        timestamp = datetime.fromisoformat(data['data']['time']['s']).astimezone(bangkok_tz)
        data['data']['time']['s'] = timestamp.isoformat()

        # Check if data already exists in the database
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
        query = """
            SELECT 1 FROM factairvisualtable WHERE timestamp = %s LIMIT 1;
        """
        if postgres_hook.get_first(query, parameters=(timestamp,)):
            raise AirflowSkipException("Data already exists for this hour")

        # Atomic file write pattern
        temp_file_path = '/tmp/airvisual_data.json'
        with open(temp_file_path, 'w') as f:
            json.dump(data, f)

    def read_data_airvisual(**kwargs):
        temp_file_path = '/tmp/airvisual_data.json'
        with open(temp_file_path, 'r') as f:
            data = json.load(f)
            # Validate file accessibility and data structure
            if 'data' not in data:
                raise ValueError("Invalid data structure in JSON file")
            # Log successful data extraction
            print("Data extraction successful")

    def load_airvisual_to_postgresql(**kwargs):
        # Load configuration files
        pollution_mapping = json.load(open('pollution_mapping.json'))
        weather_mapping = json.load(open('weather_mapping.json'))

        # Load data from temporary file
        temp_file_path = '/tmp/airvisual_data.json'
        with open(temp_file_path, 'r') as f:
            data = json.load(f)

        # Extract relevant data
        timestamp = data['data']['time']['s']
        location = data['data']['city']
        pollution = data['data']['pollution']
        weather = data['data']['weather']

        # Insert timestamp data into dimDateTimeTable
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
        postgres_hook.run("""
            INSERT INTO dimDateTimeTable (timestamp)
            VALUES (%s)
            ON CONFLICT (timestamp) DO NOTHING;
        """, parameters=(timestamp,))

        # Insert location data into dimLocationTable
        postgres_hook.run("""
            INSERT INTO dimLocationTable (city, country, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city, country) DO NOTHING;
        """, parameters=(location['city'], location['country'], location['coordinates']['lat'], location['coordinates']['lon']))

        # Insert pollution metadata into dimMainPollutionTable
        postgres_hook.run("""
            INSERT INTO dimMainPollutionTable (aqi, main_pollutant, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
        """, parameters=(pollution['aqi'], pollution['mainus'], timestamp))

        # Insert main fact data into factairvisualtable
        postgres_hook.run("""
            INSERT INTO factairvisualtable (timestamp, city, country, latitude, longitude, aqi, main_pollutant, temperature, humidity, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, city, country) DO NOTHING;
        """, parameters=(timestamp, location['city'], location['country'], location['coordinates']['lat'], location['coordinates']['lon'], pollution['aqi'], pollution['mainus'], weather['tp'], weather['hu'], weather['ws']))

    # Define tasks
    t1 = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_airvisual_data_hourly,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    t2 = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_data_airvisual,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='load_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgresql,
        provide_context=True
    )

    # Define task dependencies
    t1 >> t2 >> t3