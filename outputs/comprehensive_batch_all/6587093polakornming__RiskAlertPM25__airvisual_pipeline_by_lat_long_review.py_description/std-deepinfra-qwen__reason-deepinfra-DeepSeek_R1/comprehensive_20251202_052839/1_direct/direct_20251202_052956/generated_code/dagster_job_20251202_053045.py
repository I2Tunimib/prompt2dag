from dagster import op, job, RetryPolicy, Failure, get_dagster_logger
import requests
import json
import os
from datetime import datetime, timezone
import pytz
import psycopg2
from psycopg2.extras import execute_values

logger = get_dagster_logger()

# Resources and Configurations
# postgres_conn = {
#     'dbname': 'your_dbname',
#     'user': 'your_user',
#     'password': 'your_password',
#     'host': 'your_host',
#     'port': 'your_port'
# }

# API Key and Coordinates
# api_key = 'your_api_key'
# coordinates = {'latitude': 13.7563, 'longitude': 100.3072}  # Salaya, Nakhon Pathom, Thailand

# Configuration Files
# pollution_mapping_file = 'path/to/pollution_mapping.json'
# weather_mapping_file = 'path/to/weather_mapping.json'

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"api_key", "coordinates"}
)
def get_airvisual_data_hourly(context):
    api_key = context.resources.api_key
    coordinates = context.resources.coordinates
    url = f"https://api.airvisual.com/v2/nearest_city?lat={coordinates['latitude']}&lon={coordinates['longitude']}&key={api_key}"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    if data['status'] != 'success':
        raise Failure(f"API response validation failed: {data['status']}")
    
    data['data']['timestamp'] = convert_to_bangkok_timezone(data['data']['time'])
    
    if check_data_exists_in_db(data):
        raise Failure("Data already exists in the database for the current hour")
    
    temp_file_path = write_temp_json(data)
    return temp_file_path

def convert_to_bangkok_timezone(timestamp):
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    utc_time = datetime.fromisoformat(timestamp['iso']).replace(tzinfo=timezone.utc)
    bangkok_time = utc_time.astimezone(bangkok_tz)
    return bangkok_time.isoformat()

def check_data_exists_in_db(data):
    conn = psycopg2.connect(**context.resources.postgres_conn)
    cursor = conn.cursor()
    query = """
    SELECT 1 FROM factairvisualtable WHERE timestamp = %s;
    """
    cursor.execute(query, (data['data']['timestamp'],))
    exists = cursor.fetchone()
    cursor.close()
    conn.close()
    return exists

def write_temp_json(data):
    temp_file_path = '/tmp/airvisual_data.json'
    with open(temp_file_path, 'w') as f:
        json.dump(data, f)
    return temp_file_path

@op
def read_data_airvisual(context, temp_file_path):
    if not os.path.exists(temp_file_path):
        raise Failure(f"File not found: {temp_file_path}")
    
    with open(temp_file_path, 'r') as f:
        data = json.load(f)
    
    if 'data' not in data:
        raise Failure("Invalid data structure in JSON file")
    
    logger.info("Data extraction successful")
    return data

@op
def load_airvisual_to_postgresql(context, data):
    conn = psycopg2.connect(**context.resources.postgres_conn)
    cursor = conn.cursor()
    
    try:
        # Load pollution and weather mappings
        with open(context.resources.pollution_mapping_file, 'r') as f:
            pollution_mapping = json.load(f)
        
        with open(context.resources.weather_mapping_file, 'r') as f:
            weather_mapping = json.load(f)
        
        # Insert timestamp data
        insert_timestamp_data(cursor, data)
        
        # Insert location data
        insert_location_data(cursor, data)
        
        # Insert pollution metadata
        insert_pollution_metadata(cursor, data, pollution_mapping)
        
        # Insert main fact data
        insert_fact_data(cursor, data, weather_mapping, pollution_mapping)
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error during database operations: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def insert_timestamp_data(cursor, data):
    timestamp = data['data']['timestamp']
    query = """
    INSERT INTO dimDateTimeTable (timestamp) VALUES (%s)
    ON CONFLICT (timestamp) DO NOTHING;
    """
    cursor.execute(query, (timestamp,))

def insert_location_data(cursor, data):
    location = data['data']['city']
    query = """
    INSERT INTO dimLocationTable (location) VALUES (%s)
    ON CONFLICT (location) DO NOTHING;
    """
    cursor.execute(query, (location,))

def insert_pollution_metadata(cursor, data, pollution_mapping):
    pollution_data = data['data']['pollution']
    for key, value in pollution_data.items():
        if key in pollution_mapping:
            query = """
            INSERT INTO dimMainPollutionTable (pollutant, value) VALUES (%s, %s)
            ON CONFLICT (pollutant) DO UPDATE SET value = EXCLUDED.value;
            """
            cursor.execute(query, (pollution_mapping[key], value))

def insert_fact_data(cursor, data, weather_mapping, pollution_mapping):
    timestamp = data['data']['timestamp']
    location = data['data']['city']
    weather_data = data['data']['weather']
    pollution_data = data['data']['pollution']
    
    fact_data = {
        'timestamp': timestamp,
        'location': location,
        'temperature': weather_data['tp'],
        'humidity': weather_data['hu'],
        'wind_speed': weather_data['ws'],
        'wind_direction': weather_data['wd'],
        'pressure': weather_data['pr'],
        'pollutant_aqi': pollution_data['aqi'],
        'pollutant_pm25': pollution_data['pm25'],
        'pollutant_pm10': pollution_data['pm10'],
        'pollutant_o3': pollution_data['o3'],
        'pollutant_no2': pollution_data['no2'],
        'pollutant_so2': pollution_data['so2'],
        'pollutant_co': pollution_data['co'],
    }
    
    query = """
    INSERT INTO factairvisualtable (
        timestamp, location, temperature, humidity, wind_speed, wind_direction, pressure,
        pollutant_aqi, pollutant_pm25, pollutant_pm10, pollutant_o3, pollutant_no2, pollutant_so2, pollutant_co
    ) VALUES %s
    ON CONFLICT (timestamp, location) DO UPDATE SET
        temperature = EXCLUDED.temperature,
        humidity = EXCLUDED.humidity,
        wind_speed = EXCLUDED.wind_speed,
        wind_direction = EXCLUDED.wind_direction,
        pressure = EXCLUDED.pressure,
        pollutant_aqi = EXCLUDED.pollutant_aqi,
        pollutant_pm25 = EXCLUDED.pollutant_pm25,
        pollutant_pm10 = EXCLUDED.pollutant_pm10,
        pollutant_o3 = EXCLUDED.pollutant_o3,
        pollutant_no2 = EXCLUDED.pollutant_no2,
        pollutant_so2 = EXCLUDED.pollutant_so2,
        pollutant_co = EXCLUDED.pollutant_co;
    """
    execute_values(cursor, query, [tuple(fact_data.values())])

@job(
    resource_defs={
        "api_key": "your_api_key",
        "coordinates": {"latitude": 13.7563, "longitude": 100.3072},
        "postgres_conn": {"dbname": "your_dbname", "user": "your_user", "password": "your_password", "host": "your_host", "port": "your_port"},
        "pollution_mapping_file": "path/to/pollution_mapping.json",
        "weather_mapping_file": "path/to/weather_mapping.json"
    },
    retry_policy=RetryPolicy(max_retries=3, delay=180),
    tags={"pipeline": "airvisual_etl"}
)
def airvisual_etl_pipeline():
    temp_file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(temp_file_path)
    load_airvisual_to_postgresql(data)

if __name__ == '__main__':
    result = airvisual_etl_pipeline.execute_in_process()