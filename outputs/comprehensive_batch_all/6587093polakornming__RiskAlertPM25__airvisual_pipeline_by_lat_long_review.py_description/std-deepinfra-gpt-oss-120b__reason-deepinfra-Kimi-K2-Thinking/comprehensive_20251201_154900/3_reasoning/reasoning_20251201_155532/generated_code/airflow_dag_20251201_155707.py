from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import os
import requests
import pytz
import psycopg2
from pathlib import Path
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Constants
CONFIG_PATH = "/opt/airflow/config/config.conf"
DATA_FILE_PATH = "/opt/airflow/data/airvisual_current.json"
POLLUTION_MAP_PATH = "/opt/airflow/config/pollution_mapping.json"
WEATHER_MAP_PATH = "/opt/airflow/config/weather_mapping.json"
COORDINATES = {"lat": 13.79, "lon": 100.32}  # Salaya, Nakhon Pathom, Thailand
TIMEZONE = "Asia/Bangkok"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def get_airvisual_data_hourly(**context):
    """Fetch air quality data from AirVisual API with deduplication check."""
    # Read API key from config
    config = Path(CONFIG_PATH)
    if not config.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")
    
    with open(config, "r") as f:
        config_data = json.load(f)
        api_key = config_data.get("airvisual_api_key")
        if not api_key:
            raise ValueError("airvisual_api_key not found in config.conf")

    # Fetch data from AirVisual API
    url = "http://api.airvisual.com/v2/nearest_city"
    params = {
        "lat": COORDINATES["lat"],
        "lon": COORDINATES["lon"],
        "key": api_key
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    api_data = response.json()
    
    if api_data.get("status") != "success":
        raise ValueError(f"API returned error: {api_data.get('data', {}).get('message', 'Unknown error')}")
    
    # Convert timestamp to Bangkok timezone
    utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
    bangkok_time = utc_time.astimezone(pytz.timezone(TIMEZONE))
    current_hour = bangkok_time.replace(minute=0, second=0, microsecond=0)
    
    # Check for duplicates in database
    conn = BaseHook.get_connection("postgres_conn")
    try:
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            user=conn.login,
            password=conn.password
        ) as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM factairvisualtable WHERE timestamp = %s LIMIT 1",
                    (current_hour,)
                )
                if cursor.fetchone():
                    raise AirflowSkipException(
                        f"Data already exists for timestamp {current_hour}, skipping downstream tasks"
                    )
    except psycopg2.Error as e:
        logger.error(f"Database error during duplicate check: {e}")
        raise
    
    # Add timezone-aware timestamp to data
    api_data["bangkok_timestamp"] = current_hour.isoformat()
    
    # Atomic file write
    data_path = Path(DATA_FILE_PATH)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = data_path.with_suffix(".tmp")
    
    with open(temp_path, "w") as f:
        json.dump(api_data, f, indent=2)
    
    os.rename(temp_path, data_path)
    logger.info(f"Successfully saved AirVisual data to {DATA_FILE_PATH}")


def read_data_airvisual(**context):
    """Read and validate the saved JSON data file."""
    data_path = Path(DATA_FILE_PATH)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found at {DATA_FILE_PATH}")
    
    try:
        with open(data_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in data file: {e}")
    
    # Validate required fields
    required_fields = ["status", "data"]
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field '{field}' in API response")
    
    if data["status"] != "success":
        raise ValueError(f"API status is not success: {data.get('status')}")
    
    logger.info(f"Successfully validated data extraction from {DATA_FILE_PATH}")


def load_airvisual_to_postgresql(**context):
    """Load processed data into PostgreSQL with transaction management."""
    # Load mapping configurations
    try:
        with open(POLLUTION_MAP_PATH, "r") as f:
            pollution_mapping = json.load(f)
        with open(WEATHER_MAP_PATH, "r") as f:
            weather_mapping = json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Mapping configuration file not found: {e}")
    
    # Read extracted data
    data_path = Path(DATA_FILE_PATH)
    with open(data_path, "r") as f:
        data = json.load(f)
    
    timestamp = datetime.fromisoformat(data["bangkok_timestamp"])
    api_data = data["data"]
    
    # Extract metrics with safe defaults
    location_data = api_data.get("location", {})
    current_data = api_data.get("current", {})
    pollution_data = current_data.get("pollution", {})
    weather_data = current_data.get("weather", {})
    
    # Get PostgreSQL connection
    conn = BaseHook.get_connection("postgres_conn")
    
    try:
        with psycopg2.connect(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            user=conn.login,
            password=conn.password
        ) as db_conn:
            with db_conn.cursor() as cursor:
                # Insert into dimDateTimeTable
                cursor.execute(
                    """
                    INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING
                    """,
                    (timestamp, timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute)
                )
                
                # Insert into dimLocationTable
                cursor.execute(
                    """
                    INSERT INTO dimLocationTable (latitude, longitude, city, country)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (latitude, longitude) DO NOTHING
                    """,
                    (COORDINATES["lat"], COORDINATES["lon"], "Salaya", "Thailand")
                )
                
                # Insert into dimMainPollutionTable
                main_pollutant = pollution_data.get("mainus", "unknown")
                cursor.execute(
                    """
                    INSERT INTO dimMainPollutionTable (pollutant_code, pollutant_name)
                    VALUES (%s, %s)
                    ON CONFLICT (pollutant_code) DO NOTHING
                    """,
                    (main_pollutant, pollution_mapping.get(main_pollutant, main_pollutant))
                )
                
                # Insert into factairvisualtable
                cursor.execute(
                    """
                    INSERT INTO factairvisualtable (
                        timestamp, latitude, longitude, aqius, mainus, aqicn, maincn,
                        temperature, humidity, pressure, wind_speed, wind_direction,
                        precipitation, timestamp_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        timestamp,
                        COORDINATES["lat"],
                        COORDINATES["lon"],
                        pollution_data.get("aqius"),
                        main_pollutant,
                        pollution_data.get("aqicn"),
                        pollution_data.get("maincn"),
                        weather_data.get("tp"),
                        weather_data.get("hu"),
                        weather_data.get("pr"),
                        weather_data.get("ws"),
                        weather_data.get("wd"),
                        weather_data.get("precip"),
                        datetime.utcnow()
                    )
                )
                
                db_conn.commit()
                logger.info("Successfully loaded data into PostgreSQL")
                
    except psycopg2.Error as e:
        logger.error(f"Database error during data load: {e}")
        raise


with DAG(
    "airvisual_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for AirVisual air quality data in Salaya, Thailand",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["airquality", "etl", "thailand"],
) as dag:
    
    t1 = PythonOperator(
        task_id="get_airvisual_data_hourly",
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    t2 = PythonOperator(
        task_id="read_data_airvisual",
        python_callable=read_data_airvisual,
    )
    
    t3 = PythonOperator(
        task_id="load_airvisual_to_postgresql",
        python_callable=load_airvisual_to_postgresql,
    )
    
    t1 >> t2 >> t3