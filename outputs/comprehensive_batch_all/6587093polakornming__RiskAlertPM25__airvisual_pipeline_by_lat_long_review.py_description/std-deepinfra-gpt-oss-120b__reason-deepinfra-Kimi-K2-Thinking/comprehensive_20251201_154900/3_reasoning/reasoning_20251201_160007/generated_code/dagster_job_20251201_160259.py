from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    RetryPolicy,
    get_dagster_logger,
    In,
    Out,
    Nothing,
)
import requests
import json
import os
from datetime import datetime
import pytz
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager

# Resources and configuration would be defined here or in resources.py
# For this single module, I'll include minimal stubs

class AirVisualConfig(Config):
    """Configuration for AirVisual API"""
    api_key: str
    latitude: float = 13.8216  # Salaya, Nakhon Pathom, Thailand
    longitude: float = 100.0444
    temp_file_path: str = "/tmp/airvisual_data.json"

class DatabaseConfig(Config):
    """Configuration for PostgreSQL connection"""
    host: str = "localhost"
    port: int = 5432
    database: str = "airquality"
    user: str = "postgres"
    password: str = "password"  # In production, use secrets
    connection_id: str = "postgres_conn"

class FileConfig(Config):
    """Configuration for mapping files"""
    pollution_mapping_path: str = "/config/pollution_mapping.json"
    weather_mapping_path: str = "/config/weather_mapping.json"

# Minimal resource stubs (in a real setup, these would be separate)
# @resource
# def postgres_resource(context):
#     return DatabaseConfig()

# @resource
# def airvisual_api_resource(context):
#     return AirVisualConfig()

# @resource
# def file_config_resource(context):
#     return FileConfig()

@op(
    out={"data_fetched": Out(bool)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    config_schema=AirVisualConfig,
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> bool:
    """
    Fetches current air quality data from AirVisual API.
    Validates response, checks for duplicates, and saves to JSON file.
    Returns True if data was fetched and saved, False if duplicate.
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    # Check if data already exists in database
    # This would need DB connection - for now, implement the check
    # For simplicity, I'll implement a basic check
    try:
        # Connect to PostgreSQL
        db_config = DatabaseConfig()
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password
        )
        cursor = conn.cursor()
        
        # Get current hour in Bangkok timezone
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        current_time = datetime.now(bangkok_tz)
        current_hour = current_time.replace(minute=0, second=0, microsecond=0)
        
        # Check if data exists for current hour
        query = """
            SELECT 1 FROM factairvisualtable 
            WHERE timestamp = %s AND latitude = %s AND longitude = %s
        """
        cursor.execute(query, (current_hour, config.latitude, config.longitude))
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        if exists:
            logger.info(f"Data already exists for {current_hour}, skipping fetch")
            return False  # Signal to skip downstream ops
            
    except Exception as e:
        logger.warning(f"Could not check for duplicates: {e}. Proceeding with fetch.")
    
    # Fetch data from AirVisual API
    url = "http://api.airvisual.com/v2/nearest_city"
    params = {
        "lat": config.latitude,
        "lon": config.longitude,
        "key": config.api_key
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    # Validate response structure
    if "data" not in data:
        raise ValueError("Invalid API response: missing 'data' key")
    
    # Add metadata
    data["extracted_at"] = datetime.now(bangkok_tz).isoformat()
    data["coordinates"] = {
        "latitude": config.latitude,
        "longitude": config.longitude
    }
    
    # Atomic file write
    temp_file = config.temp_file_path + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f)
    
    os.replace(temp_file, config.temp_file_path)
    
    logger.info(f"Successfully fetched and saved data to {config.temp_file_path}")
    return True

@op(
    ins={"data_fetched": In(bool)},
    out={"validated_data": Out(Optional[Dict[str, Any]])},
    retry_policy=RetryPolicy(max_retries=0),  # No retries for this task
)
def read_data_airvisual(context: OpExecutionContext, data_fetched: bool) -> Optional[Dict[str, Any]]:
    """
    Reads and validates the saved JSON file.
    Returns the data if valid, None if skipped.
    """
    logger = get_dagster_logger()
    
    if not data_fetched:
        logger.info("Skipping data read - no new data fetched")
        return None
    
    config = AirVisualConfig()
    
    # Read and validate JSON file
    try:
        with open(config.temp_file_path, "r") as f:
            data = json.load(f)
        
        # Validate structure
        if "data" not in data:
            raise ValueError("Invalid data structure: missing 'data' key")
        
        logger.info("Successfully validated data extraction")
        return data
        
    except Exception as e:
        logger.error(f"Failed to read or validate data: {e}")
        raise

@op(
    ins={"validated_data": In(Optional[Dict[str, Any]])},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=0),  # DAG-level retries handled by job config
    config_schema={"database": DatabaseConfig, "file_config": FileConfig},
)
def load_airvisual_to_postgresql(context: OpExecutionContext, validated_data: Optional[Dict[str, Any]]):
    """
    Performs complete ETL process to load data into PostgreSQL.
    Handles dimension tables and fact table with transaction management.
    """
    logger = get_dagster_logger()
    
    if validated_data is None:
        logger.info("Skipping load - no new data to process")
        return
    
    # Get configs
    db_config = context.op_config["database"]
    file_config = context.op_config["file_config"]
    
    # Load mapping configurations
    try:
        with open(file_config.pollution_mapping_path, "r") as f:
            pollution_mapping = json.load(f)
        
        with open(file_config.weather_mapping_path, "r") as f:
            weather_mapping = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load mapping configurations: {e}")
        raise
    
    # Extract data
    data = validated_data["data"]
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    
    # Parse timestamp
    timestamp_str = data.get("current", {}).get("pollution", {}).get("ts")
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        timestamp = timestamp.astimezone(bangkok_tz)
    else:
        timestamp = datetime.now(bangkok_tz)
    
    # Prepare dimension data
    location_data = {
        "latitude": validated_data["coordinates"]["latitude"],
        "longitude": validated_data["coordinates"]["longitude"],
        "city": data.get("city", "Salaya"),
        "state": data.get("state", "Nakhon Pathom"),
        "country": data.get("country", "Thailand")
    }
    
    pollution_data = data.get("current", {}).get("pollution", {})
    weather_data = data.get("current", {}).get("weather", {})
    
    # Database operations with transaction management
    try:
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password
        )
        conn.autocommit = False
        cursor = conn.cursor()
        
        try:
            # Insert into dimDateTimeTable
            datetime_query = """
                INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
            """
            cursor.execute(datetime_query, (
                timestamp,
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                timestamp.minute
            ))
            
            # Insert into dimLocationTable
            location_query = """
                INSERT INTO dimLocationTable (latitude, longitude, city, state, country)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (latitude, longitude) DO NOTHING
            """
            cursor.execute(location_query, (
                location_data["latitude"],
                location_data["longitude"],
                location_data["city"],
                location_data["state"],
                location_data["country"]
            ))
            
            # Insert into dimMainPollutionTable
            pollution_query = """
                INSERT INTO dimMainPollutionTable 
                (main_pollutant, aqius, aqicn, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (main_pollutant) DO UPDATE SET
                aqius = EXCLUDED.aqius,
                aqicn = EXCLUDED.aqicn,
                description = EXCLUDED.description
            """
            main_pollutant = pollution_data.get("mainus", "unknown")
            cursor.execute(pollution_query, (
                main_pollutant,
                pollution_data.get("aqius"),
                pollution_data.get("aqicn"),
                pollution_mapping.get(main_pollutant, "Unknown pollutant")
            ))
            
            # Insert into factairvisualtable
            fact_query = """
                INSERT INTO factairvisualtable (
                    timestamp, latitude, longitude, main_pollutant,
                    aqius, aqicn, temperature, humidity, pressure, wind_speed,
                    weather_icon, precipitation, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, latitude, longitude) DO NOTHING
            """
            
            cursor.execute(fact_query, (
                timestamp,
                location_data["latitude"],
                location_data["longitude"],
                main_pollutant,
                pollution_data.get("aqius"),
                pollution_data.get("aqicn"),
                weather_data.get("tp"),
                weather_data.get("hu"),
                weather_data.get("pr"),
                weather_data.get("ws"),
                weather_data.get("ic"),
                weather_data.get("pr"),  # Using pressure as precipitation placeholder
                datetime.now(bangkok_tz)
            ))
            
            conn.commit()
            logger.info("Successfully loaded data into PostgreSQL")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction failed, rolled back: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@job(
    retry_policy=RetryPolicy(max_retries=0),  # DAG-level retries not directly supported, use op-level
    # In Dagster, job-level retries are configured at execution time, not definition
)
def airvisual_etl_job():
    """
    ETL pipeline for AirVisual air quality data.
    Linear execution: fetch -> read -> load
    """
    # Linear dependency chain
    data_fetched = get_airvisual_data_hourly()
    validated_data = read_data_airvisual(data_fetched)
    load_airvisual_to_postgresql(validated_data)

if __name__ == "__main__":
    # Minimal launch pattern
    # In production, use dagster.yaml and schedules
    result = airvisual_etl_job.execute_in_process(
        run_config={
            "ops": {
                "get_airvisual_data_hourly": {
                    "config": {
                        "api_key": "YOUR_API_KEY_HERE",
                        "latitude": 13.8216,
                        "longitude": 100.0444,
                        "temp_file_path": "/tmp/airvisual_data.json"
                    }
                },
                "load_airvisual_to_postgresql": {
                    "config": {
                        "database": {
                            "host": "localhost",
                            "port": 5432,
                            "database": "airquality",
                            "user": "postgres",
                            "password": "password",
                            "connection_id": "postgres_conn"
                        },
                        "file_config": {
                            "pollution_mapping_path": "/config/pollution_mapping.json",
                            "weather_mapping_path": "/config/weather_mapping.json"
                        }
                    }
                }
            }
        }
    )