from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip
import requests
import json
import psycopg2
from datetime import datetime, timezone
import pytz
import os

# Constants
API_KEY = "your_api_key"
COORDINATES = (13.7563, 100.3084)  # Salaya, Nakhon Pathom, Thailand
TIMEZONE = pytz.timezone('Asia/Bangkok')
POSTGRES_CONN_ID = "postgres_conn"
POLLUTION_CONFIG_FILE = "pollution_mapping.json"
WEATHER_CONFIG_FILE = "weather_mapping.json"
TEMP_JSON_FILE = "airvisual_data.json"

# Task: Fetch air quality data from AirVisual API
@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    logger = get_run_logger()
    lat, lon = COORDINATES
    url = f"https://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Validate API response
    if data['status'] != 'success':
        raise ValueError("API response validation failed")
    
    # Convert timestamp to Bangkok timezone
    timestamp = datetime.fromtimestamp(data['data']['current']['weather']['ts'], timezone.utc)
    timestamp_bangkok = timestamp.astimezone(TIMEZONE)
    data['data']['current']['weather']['ts'] = timestamp_bangkok.isoformat()
    
    # Check if data already exists in the database
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM factairvisualtable WHERE timestamp = %s", (timestamp_bangkok,))
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    if count > 0:
        raise Skip("Data already exists for the current hour")
    
    # Atomic file write pattern
    temp_file = f"{TEMP_JSON_FILE}.tmp"
    with open(temp_file, 'w') as f:
        json.dump(data, f)
    os.replace(temp_file, TEMP_JSON_FILE)
    
    logger.info("AirVisual data fetched and saved successfully")

# Task: Read the saved JSON file to verify data extraction success
@task
def read_data_airvisual():
    logger = get_run_logger()
    if not os.path.exists(TEMP_JSON_FILE):
        raise FileNotFoundError("JSON file not found")
    
    with open(TEMP_JSON_FILE, 'r') as f:
        data = json.load(f)
    
    # Validate file accessibility and data structure
    if 'data' not in data or 'current' not in data['data']:
        raise ValueError("Invalid data structure in JSON file")
    
    logger.info("Data extraction verified successfully")

# Task: Perform complete ETL process to load data into PostgreSQL
@task
def load_airvisual_to_postgresql():
    logger = get_run_logger()
    
    # Load configuration files
    with open(POLLUTION_CONFIG_FILE, 'r') as f:
        pollution_config = json.load(f)
    
    with open(WEATHER_CONFIG_FILE, 'r') as f:
        weather_config = json.load(f)
    
    # Load data from JSON file
    with open(TEMP_JSON_FILE, 'r') as f:
        data = json.load(f)
    
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()
    
    try:
        # Insert timestamp data into dimDateTimeTable with conflict resolution
        timestamp = data['data']['current']['weather']['ts']
        cursor.execute("""
            INSERT INTO dimDateTimeTable (timestamp) VALUES (%s)
            ON CONFLICT (timestamp) DO NOTHING
        """, (timestamp,))
        
        # Insert location data into dimLocationTable with conflict resolution
        city = data['data']['city']
        state = data['data']['state']
        country = data['data']['country']
        cursor.execute("""
            INSERT INTO dimLocationTable (city, state, country) VALUES (%s, %s, %s)
            ON CONFLICT (city, state, country) DO NOTHING
        """, (city, state, country))
        
        # Insert pollution metadata into dimMainPollutionTable with conflict resolution
        pollution_data = data['data']['current']['pollution']
        for key, value in pollution_data.items():
            if key in pollution_config:
                cursor.execute("""
                    INSERT INTO dimMainPollutionTable (pollutant, value) VALUES (%s, %s)
                    ON CONFLICT (pollutant) DO UPDATE SET value = EXCLUDED.value
                """, (key, value))
        
        # Insert main fact data into factairvisualtable with comprehensive weather and pollution metrics
        weather_data = data['data']['current']['weather']
        for key, value in weather_data.items():
            if key in weather_config:
                cursor.execute("""
                    INSERT INTO factairvisualtable (timestamp, city, state, country, metric, value)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, city, state, country, metric) DO NOTHING
                """, (timestamp, city, state, country, key, value))
        
        conn.commit()
        logger.info("Data loaded into PostgreSQL successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data into PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

# Flow: Orchestrate the ETL pipeline
@flow(retries=3, retry_delay_seconds=180)
def airvisual_etl_pipeline():
    try:
        get_airvisual_data_hourly()
        read_data_airvisual()
        load_airvisual_to_postgresql()
    except Skip as e:
        logger = get_run_logger()
        logger.warning(f"Pipeline skipped: {e}")

if __name__ == '__main__':
    airvisual_etl_pipeline()