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
}

def get_airvisual_data_hourly(**kwargs):
    api_key = kwargs['params']['api_key']
    latitude = kwargs['params']['latitude']
    longitude = kwargs['params']['longitude']
    url = f"https://api.airvisual.com/v2/nearest_city?lat={latitude}&lon={longitude}&key={api_key}"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    if 'data' not in data:
        raise ValueError("Invalid API response")
    
    data['data']['timestamp'] = convert_to_bangkok_timezone(data['data']['time'])
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM factairvisualtable WHERE timestamp = %s
            """, (data['data']['timestamp'],))
            if cursor.fetchone():
                raise AirflowSkipException("Data already exists for this hour")
    
    temp_file = '/tmp/airvisual_data.json'
    with open(temp_file, 'w') as f:
        json.dump(data, f)
    
    return temp_file

def convert_to_bangkok_timezone(timestamp):
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    return pytz.utc.localize(datetime.fromisoformat(timestamp)).astimezone(bangkok_tz).isoformat()

def read_data_airvisual(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='get_airvisual_data_hourly')
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    if 'data' not in data:
        raise ValueError("Invalid data structure")
    
    print(f"Data extraction successful: {data['data']['timestamp']}")

def load_airvisual_to_postgresql(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='get_airvisual_data_hourly')
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    with open('/path/to/pollution_mapping.json', 'r') as f:
        pollution_mapping = json.load(f)
    
    with open('/path/to/weather_mapping.json', 'r') as f:
        weather_mapping = json.load(f)
    
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute, second)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
            """, (
                data['data']['timestamp'],
                data['data']['timestamp'][:4],
                data['data']['timestamp'][5:7],
                data['data']['timestamp'][8:10],
                data['data']['timestamp'][11:13],
                data['data']['timestamp'][14:16],
                data['data']['timestamp'][17:19],
            ))
            
            cursor.execute("""
                INSERT INTO dimLocationTable (city, state, country, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city, state, country) DO NOTHING
            """, (
                data['data']['city'],
                data['data']['state'],
                data['data']['country'],
                data['data']['location']['coordinates'][1],
                data['data']['location']['coordinates'][0],
            ))
            
            cursor.execute("""
                INSERT INTO dimMainPollutionTable (pollution_type, value, unit)
                VALUES (%s, %s, %s)
                ON CONFLICT (pollution_type) DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit
            """, (
                data['data']['pollution']['pm25'],
                data['data']['pollution']['pm25'],
                'µg/m³',
            ))
            
            cursor.execute("""
                INSERT INTO factairvisualtable (timestamp, city, state, country, pollution_type, pollution_value, weather)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, city, state, country, pollution_type) DO NOTHING
            """, (
                data['data']['timestamp'],
                data['data']['city'],
                data['data']['state'],
                data['data']['country'],
                data['data']['pollution']['pm25'],
                data['data']['pollution']['pm25'],
                json.dumps(data['data']['weather']),
            ))
            
            conn.commit()

with DAG(
    dag_id='airvisual_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'api_key': 'your_api_key',
        'latitude': 13.7563,
        'longitude': 100.3082,
    },
) as dag:
    
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