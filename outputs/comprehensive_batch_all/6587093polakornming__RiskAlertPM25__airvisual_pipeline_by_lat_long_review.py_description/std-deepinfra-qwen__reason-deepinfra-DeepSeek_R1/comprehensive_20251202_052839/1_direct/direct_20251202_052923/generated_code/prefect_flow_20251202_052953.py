from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip
from prefect.tasks import task_input_hash
from datetime import datetime, timezone, timedelta
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import os

# Constants
API_KEY = "your_api_key"
COORDINATES = (13.7563, 100.3084)  # Salaya, Nakhon Pathom, Thailand
TIMEZONE_BANGKOK = timezone(timedelta(hours=7))
POSTGRES_CONN_ID = "postgres_conn"
POLLUTION_CONFIG_FILE = "pollution_mapping.json"
WEATHER_CONFIG_FILE = "weather_mapping.json"
TEMP_JSON_FILE = "airvisual_data.json"

# Task: Fetch air quality data from AirVisual API
@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def get_airvisual_data_hourly():
    logger = get_run_logger()
    url = f"https://api.airvisual.com/v2/nearest_city?lat={COORDINATES[0]}&lon={COORDINATES[1]}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Convert timestamp to Bangkok timezone
    data['data']['time'] = datetime.fromtimestamp(data['data']['time'], TIMEZONE_BANGKOK).isoformat()

    # Check if data already exists in the database
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM factairvisualtable WHERE timestamp = %s", (data['data']['time'],))
    if cursor.fetchone():
        raise Skip("Data already exists for the current hour")

    # Atomic file write pattern
    with open(TEMP_JSON_FILE, "w") as f:
        json.dump(data, f)

    logger.info("AirVisual data fetched and saved successfully")

# Task: Read the saved JSON file to verify data extraction success
@task
def read_data_airvisual():
    logger = get_run_logger()
    if not os.path.exists(TEMP_JSON_FILE):
        raise FileNotFoundError("JSON file not found")

    with open(TEMP_JSON_FILE, "r") as f:
        data = json.load(f)

    # Validate data structure
    if "data" not in data or "time" not in data["data"]:
        raise ValueError("Invalid data structure")

    logger.info("JSON file read and validated successfully")

# Task: Load data into PostgreSQL
@task
def load_airvisual_to_postgresql():
    logger = get_run_logger()
    conn = psycopg2.connect(POSTGRES_CONN_ID)
    cursor = conn.cursor()

    # Load configuration files
    with open(POLLUTION_CONFIG_FILE, "r") as f:
        pollution_config = json.load(f)
    with open(WEATHER_CONFIG_FILE, "r") as f:
        weather_config = json.load(f)

    # Read data from JSON file
    with open(TEMP_JSON_FILE, "r") as f:
        data = json.load(f)

    # Insert timestamp data into dimDateTimeTable
    timestamp = data['data']['time']
    cursor.execute(
        "INSERT INTO dimDateTimeTable (timestamp) VALUES (%s) ON CONFLICT (timestamp) DO NOTHING",
        (timestamp,)
    )

    # Insert location data into dimLocationTable
    location = data['data']['city']
    cursor.execute(
        "INSERT INTO dimLocationTable (location) VALUES (%s) ON CONFLICT (location) DO NOTHING",
        (location,)
    )

    # Insert pollution metadata into dimMainPollutionTable
    pollution_data = data['data']['pollution']
    cursor.execute(
        "INSERT INTO dimMainPollutionTable (aqi, main_pollutant) VALUES (%s, %s) ON CONFLICT (aqi, main_pollutant) DO NOTHING",
        (pollution_data['aqi'], pollution_data['mainus'])
    )

    # Insert main fact data into factairvisualtable
    weather_data = data['data']['weather']
    cursor.execute(
        "INSERT INTO factairvisualtable (timestamp, location, aqi, main_pollutant, temperature, humidity, wind_speed) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (timestamp, location) DO NOTHING",
        (timestamp, location, pollution_data['aqi'], pollution_data['mainus'], weather_data['tp'], weather_data['hu'], weather_data['ws'])
    )

    # Commit transaction
    conn.commit()
    logger.info("Data loaded into PostgreSQL successfully")

# Flow: Orchestrate the ETL process
@flow(retries=3, retry_delay_seconds=180)
def airvisual_etl_flow():
    try:
        get_airvisual_data_hourly()
        read_data_airvisual()
        load_airvisual_to_postgresql()
    except Skip as e:
        logger = get_run_logger()
        logger.warning(f"Flow skipped: {e}")

if __name__ == '__main__':
    airvisual_etl_flow()