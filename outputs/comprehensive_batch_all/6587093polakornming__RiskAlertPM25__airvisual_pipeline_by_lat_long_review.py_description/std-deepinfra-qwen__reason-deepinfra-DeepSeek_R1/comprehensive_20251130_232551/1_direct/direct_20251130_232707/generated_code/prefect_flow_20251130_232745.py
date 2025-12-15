from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip
from prefect.tasks import task_input_hash
from datetime import datetime, timezone
import pytz
import requests
import json
import os
import psycopg2
from psycopg2 import sql

# Constants
API_KEY = "your_api_key"
COORDINATES = (13.7563, 100.3084)  # Salaya, Nakhon Pathom, Thailand
TIMEZONE = pytz.timezone('Asia/Bangkok')
POSTGRES_CONN_ID = "postgres_conn"
TEMP_FILE_PATH = "/tmp/airvisual_data.json"
POLLUTION_CONFIG_PATH = "/path/to/pollution_config.json"
WEATHER_CONFIG_PATH = "/path/to/weather_config.json"

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def get_airvisual_data_hourly():
    logger = get_run_logger()
    url = f"https://api.airvisual.com/v2/nearest_city?lat={COORDINATES[0]}&lon={COORDINATES[1]}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if data['status'] != 'success':
        raise ValueError("API response validation failed")

    data['data']['timestamp'] = datetime.fromtimestamp(data['data']['time']['s'], timezone.utc).astimezone(TIMEZONE).isoformat()

    # Check if data already exists in the database
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM factairvisualtable WHERE timestamp = %s",
        (data['data']['timestamp'],)
    )
    if cursor.fetchone():
        raise Skip("Data already exists in the database for the current hour")

    # Atomic file write
    temp_file = f"{TEMP_FILE_PATH}.tmp"
    with open(temp_file, 'w') as f:
        json.dump(data, f)
    os.rename(temp_file, TEMP_FILE_PATH)

@task
def read_data_airvisual():
    logger = get_run_logger()
    if not os.path.exists(TEMP_FILE_PATH):
        raise FileNotFoundError("Temporary JSON file not found")

    with open(TEMP_FILE_PATH, 'r') as f:
        data = json.load(f)

    if 'data' not in data:
        raise ValueError("Invalid data structure in JSON file")

    logger.info("Data extraction successful")

@task
def load_airvisual_to_postgresql():
    logger = get_run_logger()
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()

    with open(POLLUTION_CONFIG_PATH, 'r') as f:
        pollution_config = json.load(f)

    with open(WEATHER_CONFIG_PATH, 'r') as f:
        weather_config = json.load(f)

    with open(TEMP_FILE_PATH, 'r') as f:
        data = json.load(f)

    timestamp = data['data']['timestamp']
    location = data['data']['city']
    pollution = data['data']['pollution']
    weather = data['data']['weather']

    try:
        # Insert timestamp data into dimDateTimeTable
        cursor.execute(
            sql.SQL("INSERT INTO dimDateTimeTable (timestamp) VALUES (%s) ON CONFLICT DO NOTHING"),
            (timestamp,)
        )

        # Insert location data into dimLocationTable
        cursor.execute(
            sql.SQL("INSERT INTO dimLocationTable (location) VALUES (%s) ON CONFLICT DO NOTHING"),
            (location,)
        )

        # Insert pollution metadata into dimMainPollutionTable
        cursor.execute(
            sql.SQL("INSERT INTO dimMainPollutionTable (pollution_data) VALUES (%s) ON CONFLICT DO NOTHING"),
            (json.dumps(pollution),)
        )

        # Insert main fact data into factairvisualtable
        cursor.execute(
            sql.SQL("INSERT INTO factairvisualtable (timestamp, location, pollution, weather) VALUES (%s, %s, %s, %s)"),
            (timestamp, location, json.dumps(pollution), json.dumps(weather))
        )

        conn.commit()
    except Exception as e:
        logger.error(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

@flow(retries=1, retry_delay_seconds=180)
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