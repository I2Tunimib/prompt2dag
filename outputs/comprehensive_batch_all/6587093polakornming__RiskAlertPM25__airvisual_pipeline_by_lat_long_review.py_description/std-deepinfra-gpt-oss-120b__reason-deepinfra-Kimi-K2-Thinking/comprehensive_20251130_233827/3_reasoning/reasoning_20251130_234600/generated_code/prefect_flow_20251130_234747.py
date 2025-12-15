import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pytz
import requests
import psycopg2
from psycopg2 import sql

from prefect import flow, task
from prefect.exceptions import PrefectException

# Custom skip exception
class SkipException(PrefectException):
    """Exception to skip downstream task execution."""
    pass

# Configuration
CONFIG_PATH = Path("config.conf")
DATA_DIR = Path("data")
MAPPINGS_DIR = Path("mappings")

# Coordinates for Salaya, Nakhon Pathom, Thailand
COORDINATES = {
    "lat": 13.8216,
    "lon": 100.3259
}

# PostgreSQL connection ID mapping
POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "airquality",
    "user": "postgres",
    "password": "postgres"
}

@task(
    name="get_airvisual_data_hourly",
    retries=2,
    retry_delay_seconds=300,  # 5 minutes
    description="Fetches current air quality data from AirVisual API"
)
def get_airvisual_data_hourly():
    """Fetches air quality data from AirVisual API and saves to JSON file."""
    # Load API key from config
    api_key = load_api_key()
    
    # Check for existing data
    if check_existing_data():
        raise SkipException("Data already exists for current hour, skipping.")
    
    # Fetch data from API
    url = f"http://api.airvisual.com/v2/nearest_city?lat={COORDINATES['lat']}&lon={COORDINATES['lon']}&key={api_key}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    # Validate response
    if data.get("status") != "success":
        raise ValueError(f"API returned error: {data.get('data', {}).get('message', 'Unknown error')}")
    
    # Convert timestamp to Bangkok timezone
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    bangkok_time = utc_now.astimezone(bangkok_tz)
    
    # Add metadata
    data["extracted_at"] = bangkok_time.isoformat()
    data["coordinates"] = COORDINATES
    
    # Atomic file write
    DATA_DIR.mkdir(exist_ok=True)
    timestamp_str = bangkok_time.strftime("%Y%m%d_%H")
    file_path = DATA_DIR / f"airvisual_data_{timestamp_str}.json"
    temp_path = file_path.with_suffix(".tmp")
    
    with open(temp_path, "w") as f:
        json.dump(data, f, indent=2)
    
    temp_path.rename(file_path)
    
    logging.info(f"Data saved to {file_path}")
    return str(file_path)

@task(name="read_data_airvisual")
def read_data_airvisual(file_path: str):
    """Reads and validates the saved JSON file."""
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(path, "r") as f:
        data = json.load(f)
    
    # Validate data structure
    if "data" not in data:
        raise ValueError("Invalid data structure: missing 'data' key")
    
    if "current" not in data["data"]:
        raise ValueError("Invalid data structure: missing 'current' key")
    
    logging.info(f"Successfully read and validated data from {file_path}")
    return data

@task(
    name="load_airvisual_to_postgresql",
    description="Loads data into PostgreSQL with full ETL process"
)
def load_airvisual_to_postgresql(data: Dict[str, Any]):
    """Performs complete ETL process to load data into PostgreSQL."""
    # Load mappings
    pollution_mapping = load_json_mapping(MAPPINGS_DIR / "pollution_mapping.json")
    weather_mapping = load_json_mapping(MAPPINGS_DIR / "weather_mapping.json")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = False
    
    try:
        with conn.cursor() as cur:
            # Extract data
            current_data = data["data"]["current"]
            bangkok_time = datetime.fromisoformat(data["extracted_at"])
            
            # Insert into dimDateTimeTable
            insert_datetime(cur, bangkok_time)
            
            # Insert into dimLocationTable
            location_data = data["data"].get("location", {})
            insert_location(cur, location_data, COORDINATES)
            
            # Insert into dimMainPollutionTable
            pollution_data = current_data.get("pollution", {})
            insert_pollution(cur, pollution_data, pollution_mapping)
            
            # Insert into factairvisualtable
            weather_data = current_data.get("weather", {})
            insert_fact(cur, bangkok_time, location_data, pollution_data, weather_data, 
                       pollution_mapping, weather_mapping, COORDINATES)
            
            conn.commit()
            logging.info("Data successfully loaded into PostgreSQL")
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading data: {e}")
        raise
    finally:
        conn.close()

def load_api_key() -> str:
    """Loads API key from config file."""
    # Minimal config parser
    with open(CONFIG_PATH, "r") as f:
        for line in f:
            if line.startswith("airvisual_api_key"):
                return line.split("=")[1].strip()
    raise ValueError("API key not found in config.conf")

def check_existing_data() -> bool:
    """Checks if data for current hour already exists in database."""
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    bangkok_time = utc_now.astimezone(bangkok_tz)
    
    current_hour = bangkok_time.replace(minute=0, second=0, microsecond=0)
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM factairvisualtable WHERE date_time = %s AND lat = %s AND lon = %s LIMIT 1",
                (current_hour, COORDINATES["lat"], COORDINATES["lon"])
            )
            return cur.fetchone() is not None
    finally:
        conn.close()

def load_json_mapping(path: Path) -> Dict[str, Any]:
    """Loads JSON mapping file."""
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)

def insert_datetime(cur, bangkok_time: datetime):
    """Inserts timestamp into dimDateTimeTable."""
    # Assuming table has columns: date_time, year, month, day, hour, minute
    current_hour = bangkok_time.replace(minute=0, second=0, microsecond=0)
    cur.execute(
        """
        INSERT INTO dimDateTimeTable (date_time, year, month, day, hour, minute)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_time) DO NOTHING
        """,
        (current_hour, current_hour.year, current_hour.month, current_hour.day, 
         current_hour.hour, current_hour.minute)
    )

def insert_location(cur, location_data: Dict[str, Any], coordinates: Dict[str, float]):
    """Inserts location data into dimLocationTable."""
    # Assuming table has columns: location_id, city, country, lat, lon
    # Using lat/lon as unique identifier
    cur.execute(
        """
        INSERT INTO dimLocationTable (location_id, city, country, lat, lon)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (location_id) DO NOTHING
        """,
        (f"{coordinates['lat']}_{coordinates['lon']}", 
         location_data.get("city", "Salaya"),
         location_data.get("country", "Thailand"),
         coordinates["lat"],
         coordinates["lon"])
    )

def insert_pollution(cur, pollution_data: Dict[str, Any], mapping: Dict[str, Any]):
    """Inserts pollution metadata into dimMainPollutionTable."""
    # Assuming table has columns: pollution_id, aqius, mainus, pollutant
    pollution_id = f"pollution_{pollution_data.get('ts', '')}"
    cur.execute(
        """
        INSERT INTO dimMainPollutionTable (pollution_id, aqius, mainus, pollutant)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pollution_id) DO NOTHING
        """,
        (pollution_id,
         pollution_data.get("aqius"),
         pollution_data.get("mainus"),
         pollution_data.get("mainus", "unknown"))
    )

def insert_fact(cur, bangkok_time: datetime, location_data: Dict[str, Any], 
                pollution_data: Dict[str, Any], weather_data: Dict[str, Any],
                pollution_mapping: Dict[str, Any], weather_mapping: Dict[str, Any],
                coordinates: Dict[str, float]):
    """Inserts main fact data into factairvisualtable."""
    # Assuming table has columns: id, date_time, location_id, pollution_id, 
    # temperature, humidity, wind_speed, pressure, etc.
    current_hour = bangkok_time.replace(minute=0, second=0, microsecond=0)
    location_id = f"{coordinates['lat']}_{coordinates['lon']}"
    pollution_id = f"pollution_{pollution_data.get('ts', '')}"
    
    cur.execute(
        """
        INSERT INTO factairvisualtable (
            date_time, location_id, pollution_id,
            temperature, humidity, wind_speed, pressure,
            aqius, mainus, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_time, location_id) DO NOTHING
        """,
        (
            current_hour,
            location_id,
            pollution_id,
            weather_data.get("tp"),
            weather_data.get("hu"),
            weather_data.get("ws"),
            weather_data.get("pr"),
            pollution_data.get("aqius"),
            pollution_data.get("mainus"),
            bangkok_time
        )
    )

@flow(
    name="airvisual_etl_pipeline",
    description="ETL pipeline for AirVisual air quality data",
    # Manual trigger - no schedule
    # retry_delay_seconds=180,  # 3 minutes for flow-level retries
    # retries=2  # Flow-level retries
)
def airvisual_etl_pipeline():
    """Main flow orchestrating the AirVisual ETL pipeline."""
    # Sequential execution
    try:
        file_path = get_airvisual_data_hourly()
        data = read_data_airvisual(file_path)
        load_airvisual_to_postgresql(data)
    except SkipException as e:
        logging.info(f"Skipping pipeline execution: {e}")
        return

if __name__ == "__main__":
    # For local execution
    airvisual_etl_pipeline()