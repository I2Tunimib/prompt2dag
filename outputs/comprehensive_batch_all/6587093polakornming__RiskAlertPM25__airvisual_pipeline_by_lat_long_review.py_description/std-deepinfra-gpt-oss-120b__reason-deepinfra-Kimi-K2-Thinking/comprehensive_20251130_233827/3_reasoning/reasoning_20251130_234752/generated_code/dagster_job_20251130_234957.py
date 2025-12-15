from dagster import (
    op,
    job,
    resource,
    Field,
    String,
    Int,
    Float,
    Bool,
    Noneable,
    Out,
    Output,
    RetryPolicy,
    RetryRequested,
    SkipReason,
    Config,
    OpExecutionContext,
    build_resources,
    In,
)
import requests
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pytz
from typing import Dict, Any, Optional
import logging

# Resources
class AirVisualAPIResource:
    def __init__(self, api_key: str, base_url: str = "http://api.airvisual.com/v2"):
        self.api_key = api_key
        self.base_url = base_url
    
    def get_current_data(self, lat: float, lon: float) -> Dict[str, Any]:
        params = {
            "lat": lat,
            "lon": lon,
            "key": self.api_key
        }
        response = requests.get(f"{self.base_url}/nearest_city", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

class PostgresResource:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def get_connection(self):
        return psycopg2.connect(self.connection_string)
    
    def execute_query(self, query: str, params=None, fetch=False):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()

class FileSystemResource:
    def __init__(self, base_path: str = "/tmp"):
        self.base_path = base_path
    
    def write_json_atomic(self, data: Dict[str, Any], filename: str) -> str:
        filepath = os.path.join(self.base_path, filename)
        temp_filepath = f"{filepath}.tmp"
        with open(temp_filepath, "w") as f:
            json.dump(data, f)
        os.replace(temp_filepath, filepath)
        return filepath
    
    def read_json(self, filename: str) -> Dict[str, Any]:
        filepath = os.path.join(self.base_path, filename)
        with open(filepath, "r") as f:
            return json.load(f)
    
    def file_exists(self, filename: str) -> bool:
        filepath = os.path.join(self.base_path, filename)
        return os.path.exists(filepath)

# Configuration
class AirVisualConfig(Config):
    api_key: str
    latitude: float = 13.8216  # Salaya, Nakhon Pathom, Thailand
    longitude: float = 100.0076
    timezone: str = "Asia/Bangkok"
    temp_file: str = "airvisual_data.json"

class PostgresConfig(Config):
    connection_string: str = "postgresql://user:password@localhost:5432/airquality"

class FileSystemConfig(Config):
    base_path: str = "/tmp/airvisual_pipeline"
    pollution_mapping_file: str = "pollution_mapping.json"
    weather_mapping_file: str = "weather_mapping.json"

# Resources with config
@resource(config_schema={
    "api_key": Field(String, is_required=True),
    "base_url": Field(String, is_required=False, default_value="http://api.airvisual.com/v2")
})
def airvisual_api_resource(context):
    return AirVisualAPIResource(
        api_key=context.resource_config["api_key"],
        base_url=context.resource_config["base_url"]
    )

@resource(config_schema={
    "connection_string": Field(String, is_required=True)
})
def postgres_resource(context):
    return PostgresResource(
        connection_string=context.resource_config["connection_string"]
    )

@resource(config_schema={
    "base_path": Field(String, is_required=False, default_value="/tmp/airvisual_pipeline")
})
def file_system_resource(context):
    return FileSystemResource(
        base_path=context.resource_config["base_path"]
    )

# Ops
@op(
    required_resource_keys={"airvisual_api", "postgres", "file_system"},
    config_schema={
        "latitude": Field(Float, is_required=False, default_value=13.8216),
        "longitude": Field(Float, is_required=False, default_value=100.0076),
        "timezone": Field(String, is_required=False, default_value="Asia/Bangkok"),
        "temp_file": Field(String, is_required=False, default_value="airvisual_data.json")
    },
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,  # 5 minutes in seconds
    ),
    out=Out(str, description="Path to saved JSON file")
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> str:
    """
    Fetches current air quality data from AirVisual API, validates response,
    checks for duplicates, and saves to JSON file atomically.
    """
    # Extract config
    lat = context.op_config["latitude"]
    lon = context.op_config["longitude"]
    timezone_str = context.op_config["timezone"]
    temp_file = context.op_config["temp_file"]
    
    # Get resources
    api = context.resources.airvisual_api
    postgres = context.resources.postgres
    fs = context.resources.file_system
    
    # Fetch data from API
    context.log.info(f"Fetching AirVisual data for coordinates: {lat}, {lon}")
    try:
        data = api.get_current_data(lat, lon)
    except requests.exceptions.RequestException as e:
        context.log.error(f"API request failed: {e}")
        raise RetryRequested() from e
    
    # Validate API response
    if "data" not in data or "current" not in data.get("data", {}):
        raise ValueError("Invalid API response structure")
    
    # Convert timestamp to Bangkok timezone
    utc_time = datetime.utcnow()
    bangkok_tz = pytz.timezone(timezone_str)
    bangkok_time = utc_time.replace(tzinfo=pytz.utc).astimezone(bangkok_tz)
    current_hour = bangkok_time.replace(minute=0, second=0, microsecond=0)
    
    # Check if data already exists in database for current hour
    check_query = """
        SELECT 1 FROM factairvisualtable 
        WHERE latitude = %s AND longitude = %s 
        AND DATE_TRUNC('hour', timestamp) = %s
        LIMIT 1
    """
    try:
        existing = postgres.execute_query(
            check_query, 
            (lat, lon, current_hour), 
            fetch=True
        )
        if existing:
            context.log.info(f"Data already exists for {current_hour}, skipping")
            # In Dagster, we use SkipReason or raise a specific exception
            # For this case, we'll raise a custom exception that Dagster can handle
            raise Exception("Data already exists for current hour")
    except Exception as e:
        if "already exists" in str(e):
            raise
        context.log.error(f"Database check failed: {e}")
        raise RetryRequested() from e
    
    # Add metadata to data
    data["extracted_at"] = utc_time.isoformat()
    data["bangkok_time"] = bangkok_time.isoformat()
    data["coordinates"] = {"lat": lat, "lon": lon}
    
    # Atomic file write
    try:
        file_path = fs.write_json_atomic(data, temp_file)
        context.log.info(f"Data saved to {file_path}")
        return file_path
    except Exception as e:
        context.log.error(f"Failed to write data to file: {e}")
        raise RetryRequested() from e

@op(
    required_resource_keys={"file_system"},
    ins={"file_path": In(str)},
    out=Out(Dict[str, Any], description="Validated data from JSON file")
)
def read_data_airvisual(context: OpExecutionContext, file_path: str) -> Dict[str, Any]:
    """
    Reads and validates the saved JSON file.
    """
    fs = context.resources.file_system
    
    # Validate file accessibility
    if not fs.file_exists(os.path.basename(file_path)):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Read and validate data structure
    try:
        data = fs.read_json(os.path.basename(file_path))
        
        # Validate required fields
        required_fields = ["data", "extracted_at", "bangkok_time"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        context.log.info(f"Successfully read and validated data from {file_path}")
        context.log.info(f"Data extracted at: {data['extracted_at']}")
        
        return data
    except json.JSONDecodeError as e:
        context.log.error(f"Invalid JSON format: {e}")
        raise
    except Exception as e:
        context.log.error(f"Failed to read data: {e}")
        raise

@op(
    required_resource_keys={"postgres", "file_system"},
    ins={"data": In(Dict[str, Any])},
    config_schema={
        "pollution_mapping_file": Field(String, is_required=False, default_value="pollution_mapping.json"),
        "weather_mapping_file": Field(String, is_required=False, default_value="weather_mapping.json")
    },
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,  # 3 minutes in seconds
    )
)
def load_airvisual_to_postgresql(context: OpExecutionContext, data: Dict[str, Any]) -> None:
    """
    Performs complete ETL process to load data into PostgreSQL with dimension tables.
    """
    postgres = context.resources.postgres
    fs = context.resources.file_system
    
    pollution_mapping_file = context.op_config["pollution_mapping_file"]
    weather_mapping_file = context.op_config["weather_mapping_file"]
    
    # Load mapping configurations
    try:
        pollution_mapping = fs.read_json(pollution_mapping_file)
        weather_mapping = fs.read_json(weather_mapping_file)
    except Exception as e:
        context.log.error(f"Failed to load mapping files: {e}")
        raise
    
    # Extract data
    current_data = data["data"]["current"]
    bangkok_time = datetime.fromisoformat(data["bangkok_time"])
    coordinates = data["coordinates"]
    
    # Start transaction
    conn = postgres.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Insert timestamp data into dimDateTimeTable
                datetime_query = """
                    INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute, day_of_week)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING
                """
                cur.execute(datetime_query, (
                    bangkok_time,
                    bangkok_time.year,
                    bangkok_time.month,
                    bangkok_time.day,
                    bangkok_time.hour,
                    bangkok_time.minute,
                    bangkok_time.weekday()
                ))
                
                # Insert location data into dimLocationTable
                location_query = """
                    INSERT INTO dimLocationTable (latitude, longitude, city, country)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (latitude, longitude) DO NOTHING
                """
                cur.execute(location_query, (
                    coordinates["lat"],
                    coordinates["lon"],
                    "Salaya",
                    "Thailand"
                ))
                
                # Insert pollution metadata into dimMainPollutionTable
                main_pollutant = current_data.get("pollution", {}).get("mainus", "unknown")
                pollution_query = """
                    INSERT INTO dimMainPollutionTable (pollutant_code, pollutant_name, description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (pollutant_code) DO NOTHING
                """
                cur.execute(pollution_query, (
                    main_pollutant,
                    pollution_mapping.get(main_pollutant, {}).get("name", main_pollutant),
                    pollution_mapping.get(main_pollutant, {}).get("description", "")
                ))
                
                # Insert main fact data into factairvisualtable
                pollution = current_data.get("pollution", {})
                weather = current_data.get("weather", {})
                
                fact_query = """
                    INSERT INTO factairvisualtable (
                        timestamp, latitude, longitude, aqi_us, main_pollutant,
                        temperature, humidity, pressure, wind_speed, wind_direction,
                        pm25, pm10, o3, no2, so2, co
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, latitude, longitude) DO NOTHING
                """
                
                cur.execute(fact_query, (
                    bangkok_time,
                    coordinates["lat"],
                    coordinates["lon"],
                    pollution.get("aqius"),
                    main_pollutant,
                    weather.get("tp"),
                    weather.get("hu"),
                    weather.get("pr"),
                    weather.get("ws"),
                    weather.get("wd"),
                    pollution.get("p2"),
                    pollution.get("p1"),
                    pollution.get("o3"),
                    pollution.get("n2"),
                    pollution.get("s2"),
                    pollution.get("co")
                ))
                
        context.log.info("Successfully loaded data into PostgreSQL")
        
    except Exception as e:
        context.log.error(f"Failed to load data into PostgreSQL: {e}")
        conn.rollback()
        raise RetryRequested() from e
    finally:
        conn.close()

# Job
@job(
    resource_defs={
        "airvisual_api": airvisual_api_resource,
        "postgres": postgres_resource,
        "file_system": file_system_resource
    },
    config={
        "resources": {
            "airvisual_api": {
                "config": {
                    "api_key": "YOUR_API_KEY_HERE"
                }
            },
            "postgres": {
                "config": {
                    "connection_string": "postgresql://user:password@localhost:5432/airquality"
                }
            },
            "file_system": {
                "config": {
                    "base_path": "/tmp/airvisual_pipeline"
                }
            }
        },
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "latitude": 13.8216,
                    "longitude": 100.0076,
                    "timezone": "Asia/Bangkok",
                    "temp_file": "airvisual_data.json"
                }
            },
            "load_airvisual_to_postgresql": {
                "config": {
                    "pollution_mapping_file": "pollution_mapping.json",
                    "weather_mapping_file": "weather_mapping.json"
                }
            }
        }
    },
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,  # 3 minutes for DAG-level retries
    )
)
def airvisual_etl_job():
    """
    ETL pipeline for AirVisual air quality data.
    Linear execution: get data → read/validate → load to PostgreSQL
    """
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)

# Launch pattern
if __name__ == "__main__":
    # Example of how to execute the job
    result = airvisual_etl_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print(f"Pipeline failed: {result.failure_data}")