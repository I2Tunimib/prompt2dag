import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytz
import requests
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
import psycopg2
from psycopg2.extras import execute_values
from configparser import ConfigParser

# Configuration
CONFIG_FILE = "config.conf"
COORDINATES = {"lat": 13.8231, "lon": 100.0483}  # Salaya, Nakhon Pathom, Thailand
DATA_FILE = "/tmp/airvisual_data.json"
TIMEZONE = "Asia/Bangkok"

def load_config():
    """Load configuration from config.conf"""
    config = ConfigParser()
    config.read(CONFIG_FILE)
    return {
        "api_key": config.get("airvisual", "api_key"),
        "db_host": config.get("postgres_conn", "host"),
        "db_port": config.get("postgres_conn", "port"),
        "db_name": config.get("postgres_conn", "dbname"),
        "db_user": config.get("postgres_conn", "user"),
        "db_password": config.get("postgres_conn", "password"),
    }

@task(
    name="get_airvisual_data_hourly",
    retries=2,
    retry_delay_seconds=300,  # 5 minutes
    cache_key_fn=task_input_hash,
)
def get_airvisual_data_hourly():
    """Fetch air quality data from AirVisual API and check for duplicates."""
    logger = get_run_logger()
    config = load_config()
    
    # Fetch data from API
    url = "http://api.airvisual.com/v2/nearest_city"
    params = {
        "lat": COORDINATES["lat"],
        "lon": COORDINATES["lon"],
        "key": config["api_key"],
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Validate response
    if data.get("status") != "success":
        raise ValueError(f"API returned error: {data.get('data', {}).get('message', 'Unknown error')}")
    
    # Convert timestamp to Bangkok timezone
    utc_time = datetime.utcfromtimestamp(data["data"]["current"]["ts"])
    bangkok_tz = pytz.timezone(TIMEZONE)
    bangkok_time = utc_time.replace(tzinfo=pytz.utc).astimezone(bangkok_tz)
    
    # Check for duplicates in database
    conn = psycopg2.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM factairvisualtable 
                WHERE timestamp = %s AND latitude = %s AND longitude = %s
                LIMIT 1
                """,
                (bangkok_time, COORDINATES["lat"], COORDINATES["lon"]),
            )
            if cur.fetchone():
                logger.info("Data already exists for this hour, skipping downstream tasks")
                return {"skip": True, "message": "Data already exists"}
    finally:
        conn.close()
    
    # Add timezone info to data
    data["bangkok_timestamp"] = bangkok_time.isoformat()
    
    # Atomic file write
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp_file:
        json.dump(data, tmp_file)
        tmp_path = tmp_file.name
    
    # Rename to final location (atomic on Unix)
    os.rename(tmp_path, DATA_FILE)
    
    logger.info(f"Successfully fetched and saved data for {bangkok_time}")
    return {"skip": False, "file": DATA_FILE}

@task(name="read_data_airvisual")
def read_data_airvisual():
    """Read and validate the saved JSON file."""
    logger = get_run_logger()
    
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    
    # Validate data structure
    required_keys = ["status", "data"]
    for key in required_keys:
        if key not in data:
            raise ValueError(f"Invalid data structure: missing '{key}'")
    
    logger.info("Data file validated successfully")
    return data

@task(name="load_airvisual_to_postgresql")
def load_airvisual_to_postgresql(data: dict):
    """Load data into PostgreSQL with dimension and fact table inserts."""
    logger = get_run_logger()
    config = load_config()
    
    # Load mapping configurations
    pollution_map_file = Path("pollution_mapping.json")
    weather_map_file = Path("weather_mapping.json")
    
    pollution_mapping = {}
    weather_mapping = {}
    
    if pollution_map_file.exists():
        with open(pollution_map_file, "r") as f:
            pollution_mapping = json.load(f)
    
    if weather_map_file.exists():
        with open(weather_map_file, "r") as f:
            weather_mapping = json.load(f)
    
    # Parse timestamp
    bangkok_time = datetime.fromisoformat(data["bangkok_timestamp"])
    
    # Database connection with transaction management
    conn = psycopg2.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"],
    )
    
    try:
        with conn.cursor() as cur:
            # Insert into dimDateTimeTable
            execute_values(
                cur,
                """
                INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute, second)
                VALUES %s
                ON CONFLICT (timestamp) DO NOTHING
                """,
                [(
                    bangkok_time,
                    bangkok_time.year,
                    bangkok_time.month,
                    bangkok_time.day,
                    bangkok_time.hour,
                    bangkok_time.minute,
                    bangkok_time.second,
                )],
            )
            
            # Insert into dimLocationTable
            execute_values(
                cur,
                """
                INSERT INTO dimLocationTable (latitude, longitude, city, country)
                VALUES %s
                ON CONFLICT (latitude, longitude) DO NOTHING
                """,
                [(
                    COORDINATES["lat"],
                    COORDINATES["lon"],
                    "Salaya",
                    "Thailand",
                )],
            )
            
            # Insert into dimMainPollutionTable
            pollution_type = data["data"]["current"]["pollution"].get("mainus", "PM2.5")
            execute_values(
                cur,
                """
                INSERT INTO dimMainPollutionTable (pollution_type, description)
                VALUES %s
                ON CONFLICT (pollution_type) DO NOTHING
                """,
                [(pollution_type, pollution_mapping.get(pollution_type, ""))],
            )
            
            # Insert into factairvisualtable
            current = data["data"]["current"]
            pollution = current["pollution"]
            weather = current["weather"]
            
            execute_values(
                cur,
                """
                INSERT INTO factairvisualtable (
                    timestamp, latitude, longitude, aqius, mainus, aqicn, maincn,
                    humidity, temperature, pressure, wind_speed, wind_direction,
                    precipitation, created_at
                )
                VALUES %s
                """,
                [(
                    bangkok_time,
                    COORDINATES["lat"],
                    COORDINATES["lon"],
                    pollution.get("aqius"),
                    pollution.get("mainus"),
                    pollution.get("aqicn"),
                    pollution.get("maincn"),
                    weather.get("hu"),
                    weather.get("tp"),
                    weather.get("pr"),
                    weather.get("ws"),
                    weather.get("wd"),
                    weather.get("precip"),
                    datetime.utcnow(),
                )],
            )
        
        conn.commit()
        logger.info("Data loaded successfully into PostgreSQL")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        conn.close()

@flow(
    name="airvisual-etl-pipeline",
    retries=2,
    retry_delay_seconds=180,  # 3 minutes
)
def airvisual_etl_flow():
    """Main ETL flow for AirVisual data pipeline."""
    # Step 1: Get data
    result = get_airvisual_data_hourly()
    
    # Check if we should skip downstream tasks
    if result.get("skip"):
        return {"status": "skipped", "reason": result.get("message")}
    
    # Step 2: Read and validate data
    data = read_data_airvisual()
    
    # Step 3: Load to PostgreSQL
    load_airvisual_to_postgresql(data)
    
    return {"status": "completed"}

if __name__ == "__main__":
    # Manual trigger execution
    airvisual_etl_flow()