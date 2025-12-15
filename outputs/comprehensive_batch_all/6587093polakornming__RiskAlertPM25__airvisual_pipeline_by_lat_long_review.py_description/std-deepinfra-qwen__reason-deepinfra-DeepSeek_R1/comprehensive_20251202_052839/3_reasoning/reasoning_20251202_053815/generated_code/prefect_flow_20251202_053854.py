from prefect import flow, task
from prefect.exceptions import SkipException
from datetime import datetime, timedelta
import json
import os
import pytz
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any

# Configuration (would normally come from config files/environment variables)
API_KEY = "your_api_key"
LATITUDE = "13.7890"
LONGITUDE = "100.5123"
POSTGRES_CONN_STR = "postgresql://user:pass@localhost:5432/db"
DATA_PATH = "airvisual_data.json"
CONFIG_PATH = "mappings/"

@task(
    retries=2,
    retry_delay=300,
    name="Fetch AirVisual Data",
    description="Fetches current air quality data from AirVisual API"
)
def get_airvisual_data_hourly() -> Dict[str, Any]:
    """Fetch and validate air quality data from AirVisual API"""
    url = f"http://api.airvisual.com/v2/nearest_city?lat={LATITUDE}&lon={LONGITUDE}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    if data.get("status") != "success":
        raise ValueError("Invalid API response")
    
    # Convert timestamp to Bangkok timezone
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    timestamp = datetime.now(bangkok_tz).replace(minute=0, second=0, microsecond=0)
    data["data"]["timestamp"] = timestamp.isoformat()
    
    # Check for existing data (simplified example)
    if check_existing_data(timestamp):
        raise SkipException("Data already exists for current hour")
    
    # Atomic write pattern
    temp_path = f"{DATA_PATH}.tmp"
    with open(temp_path, "w") as f:
        json.dump(data, f)
    os.rename(temp_path, DATA_PATH)
    
    return data

def check_existing_data(timestamp: datetime) -> bool:
    """Simplified existence check (would normally query database)"""
    return False  # Implement actual DB check

@task(
    name="Validate Extracted Data",
    description="Verifies data file accessibility and structure"
)
def read_data_airvisual() -> Dict[str, Any]:
    """Validate extracted data file"""
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError("Data file missing")
    
    with open(DATA_PATH, "r") as f:
        data = json.load(f)
    
    if not all(key in data.get("data", {}) for key in ["timestamp", "pollution", "weather"]):
        raise ValueError("Invalid data structure")
    
    return data

@task(
    name="Load to PostgreSQL",
    description="Performs ETL load into PostgreSQL with transaction management"
)
def load_airvisual_to_postgresql(data: Dict[str, Any]) -> None:
    """Load transformed data into PostgreSQL database"""
    engine = create_engine(POSTGRES_CONN_STR)
    
    try:
        with engine.connect() as conn:
            transaction = conn.begin()
            
            # Load mapping files
            pollution_map = load_json_config(f"{CONFIG_PATH}pollution_mapping.json")
            weather_map = load_json_config(f"{CONFIG_PATH}weather_mapping.json")
            
            # Insert dimension data
            timestamp_id = insert_timestamp(conn, data["data"]["timestamp"])
            location_id = insert_location(conn, data["data"]["location"])
            pollution_id = insert_pollution_metadata(conn, data["data"]["pollution"])
            
            # Insert fact data
            insert_fact_data(
                conn,
                data["data"],
                timestamp_id,
                location_id,
                pollution_id,
                pollution_map,
                weather_map
            )
            
            transaction.commit()
    except SQLAlchemyError as e:
        transaction.rollback()
        raise e

def load_json_config(path: str) -> Dict[str, Any]:
    """Load JSON configuration mapping file"""
    with open(path, "r") as f:
        return json.load(f)

def insert_timestamp(conn, timestamp: str) -> int:
    """Insert timestamp with conflict resolution"""
    result = conn.execute(
        text("""
        INSERT INTO dimDateTimeTable (timestamp)
        VALUES (:ts)
        ON CONFLICT (timestamp) DO NOTHING
        RETURNING id
        """),
        {"ts": timestamp}
    )
    return result.scalar() or get_existing_timestamp_id(conn, timestamp)

def get_existing_timestamp_id(conn, timestamp: str) -> int:
    result = conn.execute(
        text("SELECT id FROM dimDateTimeTable WHERE timestamp = :ts"),
        {"ts": timestamp}
    )
    return result.scalar()

def insert_location(conn, location: Dict[str, Any]) -> int:
    """Insert location data with conflict resolution"""
    result = conn.execute(
        text("""
        INSERT INTO dimLocationTable (coordinates, city, country)
        VALUES (ST_MakePoint(:lat, :lon), :city, :country)
        ON CONFLICT (coordinates) DO NOTHING
        RETURNING id
        """),
        {
            "lat": location.get("latitude"),
            "lon": location.get("longitude"),
            "city": location.get("city"),
            "country": location.get("country")
        }
    )
    return result.scalar() or get_existing_location_id(conn, location)

def get_existing_location_id(conn, location: Dict[str, Any]) -> int:
    result = conn.execute(
        text("""
        SELECT id FROM dimLocationTable
        WHERE coordinates = ST_MakePoint(:lat, :lon)
        """),
        {
            "lat": location.get("latitude"),
            "lon": location.get("longitude")
        }
    )
    return result.scalar()

def insert_pollution_metadata(conn, pollution: Dict[str, Any]) -> int:
    """Insert pollution metadata with conflict resolution"""
    result = conn.execute(
        text("""
        INSERT INTO dimMainPollutionTable (main_ pollutant, aqius, aqicn)
        VALUES (:pollutant, :aqius, :aqicn)
        ON CONFLICT (main_pollutant) DO UPDATE SET
            aqius = EXCLUDED.aqius,
            aqicn = EXCLUDED.aqicn
        RETURNING id
        """),
        pollution
    )
    return result.scalar()

def insert_fact_data(conn, data: Dict[str, Any], ts_id: int, loc_id: int, poll_id: int,
                    pollution_map: Dict, weather_map: Dict) -> None:
    """Insert transformed fact data"""
    transformed = {
        "timestamp_id": ts_id,
        "location_id": loc_id,
        "pollution_id": poll_id,
        **transform_metrics(data["pollution"], pollution_map),
        **transform_metrics(data["weather"], weather_map)
    }
    
    conn.execute(
        text("""
        INSERT INTO factairvisualtable
        (timestamp_id, location_id, pollution_id, temp, humidity, windspeed, aqi)
        VALUES
        (:timestamp_id, :location_id, :pollution_id, :temp, :humidity, :windspeed, :aqi)
        """),
        transformed
    )

def transform_metrics(data: Dict[str, Any], mapping: Dict) -> Dict[str, Any]:
    """Apply metric transformations using mapping config"""
    return {mapping.get(k, k): v for k, v in data.items()}

@flow(
    name="AirQuality ETL Pipeline",
    retries=3,
    retry_delay=180,
    description="Orchestrates air quality data ETL process"
)
def airquality_etl_flow():
    """Main ETL workflow orchestration"""
    raw_data = get_airvisual_data_hourly()
    validated_data = read_data_airvisual(raw_data)
    load_airvisual_to_postgresql(validated_data)

if __name__ == "__main__":
    airquality_etl_flow()