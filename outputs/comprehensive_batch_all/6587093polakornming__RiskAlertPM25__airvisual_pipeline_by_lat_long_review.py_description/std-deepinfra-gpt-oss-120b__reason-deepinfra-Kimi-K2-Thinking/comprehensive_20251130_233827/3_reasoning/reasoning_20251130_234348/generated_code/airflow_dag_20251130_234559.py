import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

API_ENDPOINT = "http://api.airvisual.com/v2/nearest_city"
LATITUDE = 13.7999
LONGITUDE = 100.3233
TIMEZONE = "Asia/Bangkok"
CONFIG_PATH = "/opt/airflow/config/config.conf"
DATA_FILE_PATH = "/tmp/airvisual_data.json"
POLLUTION_MAPPING_PATH = "/opt/airflow/config/pollution_mapping.json"
WEATHER_MAPPING_PATH = "/opt/airflow/config/weather_mapping.json"
POSTGRES_CONN_ID = "postgres_conn"

def get_api_key():
    """Retrieve API key from config file."""
    import configparser
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config['airvisual']['api_key']

def check_duplicate_data(timestamp, location_id):
    """Check if data for the given timestamp and location already exists."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
    SELECT 1 FROM factairvisualtable 
    WHERE timestamp = %s AND location_id = %s
    LIMIT 1
    """
    result = hook.get_first(sql, parameters=(timestamp, location_id))
    return result is not None

def get_airvisual_data_hourly(**context):
    """Fetch air quality data from AirVisual API and save to file."""
    api_key = get_api_key()
    
    params = {
        'lat': LATITUDE,
        'lon': LONGITUDE,
        'key': api_key
    }
    
    response = requests.get(API_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    if 'data' not in data or 'current' not in data['data']:
        raise ValueError("Invalid API response structure")
    
    utc_time = pendulum.parse(data['data']['current']['ts'])
    bangkok_time = utc_time.in_timezone(TIMEZONE)
    current_hour = bangkok_time.start_of('hour')
    
    location_id = f"{LATITUDE}_{LONGITUDE}"
    if check_duplicate_data(current_hour.isoformat(), location_id):
        logging.info(f"Data already exists for {current_hour} at location {location_id}")
        raise AirflowSkipException("Data already processed for this hour")
    
    data['processed_timestamp'] = current_hour.isoformat()
    data['location_id'] = location_id
    
    with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as tmp_file:
        json.dump(data, tmp_file)
        tmp_path = tmp_file.name
    
    os.rename(tmp_path, DATA_FILE_PATH)
    logging.info(f"Data saved to {DATA_FILE_PATH}")

def read_data_airvisual(**context):
    """Read and validate the saved JSON data file."""
    if not os.path.exists(DATA_FILE_PATH):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE_PATH}")
    
    with open(DATA_FILE_PATH, 'r') as f:
        data = json.load(f)
    
    required_keys = ['data', 'processed_timestamp', 'location_id']
    for key in required_keys:
        if key not in data:
            raise ValueError(f"Missing required key: {key}")
    
    if 'current' not in data['data']:
        raise ValueError("Invalid data structure: missing 'current' key")
    
    logging.info(f"Successfully validated data file with timestamp {data['processed_timestamp']}")

def load_mapping_config(file_path):
    """Load mapping configuration from JSON file."""
    if not os.path.exists(file_path):
        logging.warning(f"Mapping file not found: {file_path}, using empty mapping")
        return {}
    
    with open(file_path, 'r') as f:
        return json.load(f)

def load_airvisual_to_postgresql(**context):
    """Load air quality data into PostgreSQL database."""
    with open(DATA_FILE_PATH, 'r') as f:
        data = json.load(f)
    
    pollution_mapping = load_mapping_config(POLLUTION_MAPPING_PATH)
    weather_mapping = load_mapping_config(WEATHER_MAPPING_PATH)
    
    current = data['data']['current']
    pollution = current.get('pollution', {})
    weather = current.get('weather', {})
    timestamp = data['processed_timestamp']
    location_id = data['location_id']
    
    main_pollutant_code = pollution.get('mainus', 'p2')
    main_pollutant_name = pollution_mapping.get(main_pollutant_code, 'PM2.5')
    
    aqi_us = pollution.get('aqius', 0)
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        dt = pendulum.parse(timestamp)
        cursor.execute("""
            INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute, second)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING