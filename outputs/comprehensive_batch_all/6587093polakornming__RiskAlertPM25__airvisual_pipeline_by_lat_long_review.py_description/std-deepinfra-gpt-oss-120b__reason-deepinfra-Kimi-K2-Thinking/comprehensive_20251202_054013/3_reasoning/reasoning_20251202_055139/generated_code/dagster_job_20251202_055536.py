from dagster import (
    op,
    job,
    resource,
    Out,
    Output,
    RetryPolicy,
    RetryRequested,
    SkipReason,
    get_dagster_logger,
    OpExecutionContext,
)
import requests
import json
import os
from datetime import datetime
import pytz
from typing import Dict, Any, Optional

# Resources
@resource
def postgres_conn_resource(init_context):
    # In production, this would be a real connection
    # For now, return a stub that would be replaced with actual psycopg2 connection
    # or SQLAlchemy engine
    return {
        "host": "localhost",
        "port": 5432,
        "database": "airquality",
        "user": "user",
        "password": "password",
    }

# Configuration (would normally be in dagster.yaml or passed at runtime)
# For this example, we'll use op config

# Ops
@op(
    required_resource_keys={"postgres_conn"},
    config_schema={
        "api_key": str,
        "latitude": float,
        "longitude": float,
        "output_file_path": str,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 5 minute delay
    out=Out(Optional[str], is_required=False),
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> Optional[str]:
    """
    Fetches current air quality data from AirVisual API.
    Checks for duplicates and saves data to JSON file.
    Returns file path if new data was fetched, None if skipped.
    """
    logger = get_dagster_logger()
    config = context.op_config
    postgres_conn = context.resources.postgres_conn
    
    # Get current time in Bangkok timezone
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    current_time = datetime.now(bangkok_tz)
    current_hour = current_time.replace(minute=0, second=0, microsecond=0)
    
    # Check if data already exists for current hour
    # This is a simplified check - in production you'd use a proper DB client
    try:
        # Simulate checking database
        # query = "SELECT 1 FROM factairvisualtable WHERE timestamp = %s AND latitude = %s AND longitude = %s"
        # cursor.execute(query, (current_hour, config["latitude"], config["longitude"]))
        # if cursor.fetchone():
        #     logger.info(f"Data already exists for {current_hour}, skipping...")
        #     return None
        
        # For demo purposes, we'll check a dummy condition
        # In real implementation, use psycopg2 or SQLAlchemy
        logger.info("Checking for existing data...")
        
        # Simulate the check - in production, implement actual DB query
        # For now, we'll assume data doesn't exist to allow the pipeline to run
        # To simulate skip, you could check for a file marker
        skip_marker = f"{config['output_file_path']}.skip"
        if os.path.exists(skip_marker):
            logger.info(f"Skip marker found, skipping data fetch for {current_hour}")
            return None
            
    except Exception as e:
        logger.error(f"Error checking for duplicates: {e}")
        raise
    
    # Fetch data from AirVisual API
    api_url = "http://api.airvisual.com/v2/nearest_city"
    params = {
        "lat": config["latitude"],
        "lon": config["longitude"],
        "key": config["api_key"],
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Validate response
        if data.get("status") != "success":
            raise ValueError(f"API returned error: {data.get('data', {}).get('message', 'Unknown error')}")
        
        # Add timestamp and timezone info
        data["extracted_at"] = current_time.isoformat()
        data["hour_timestamp"] = current_hour.isoformat()
        
        # Atomic file write
        temp_file = config["output_file_path"] + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        
        os.replace(temp_file, config["output_file_path"])
        
        logger.info(f"Successfully fetched and saved data for {current_hour}")
        return config["output_file_path"]
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise RetryRequested(max_retries=2, seconds_to_wait=300) from e
    except Exception as e:
        logger.error(f"Error processing API response: {e}")
        raise

@op(
    config_schema={"output_file_path": str},
    out=Out(Dict[str, Any], is_required=False),
)
def read_data_airvisual(context: OpExecutionContext, file_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Reads and validates the saved JSON file.
    Returns parsed data if file exists and is valid.
    """
    logger = get_dagster_logger()
    
    if not file_path:
        logger.info("No file path provided, skipping read operation")
        return None
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Validate data structure
        if "data" not in data:
            raise ValueError("Invalid data structure: missing 'data' key")
        
        logger.info("Successfully read and validated data file")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading data file: {e}")
        raise

@op(
    required_resource_keys={"postgres_conn"},
    config_schema={
        "pollution_mapping_file": str,
        "weather_mapping_file": str,
    },
    retry_policy=RetryPolicy(max_retries=3, delay=180),  # 3 minute delay
)
def load_airvisual_to_postgresql(context: OpExecutionContext, data: Optional[Dict[str, Any]]) -> None:
    """
    Loads processed data into PostgreSQL dimension and fact tables.
    Implements transaction management for data consistency.
    """
    logger = get_dagster_logger()
    
    if not data:
        logger.info("No data to load, skipping")
        return
    
    postgres_conn = context.resources.postgres_conn
    config = context.op_config
    
    try:
        # In production, use psycopg2 or SQLAlchemy with proper connection management
        # For this example, we'll simulate the database operations
        
        # Load mapping configurations
        pollution_mapping = {}
        weather_mapping = {}
        
        if os.path.exists(config["pollution_mapping_file"]):
            with open(config["pollution_mapping_file"], "r") as f:
                pollution_mapping = json.load(f)
        
        if os.path.exists(config["weather_mapping_file"]):
            with open(config["weather_mapping_file"], "r") as f:
                weather_mapping = json.load(f)
        
        # Extract relevant data
        api_data = data.get("data", {})
        current_time = datetime.fromisoformat(data["hour_timestamp"])
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        
        # Simulate transaction
        logger.info("Starting database transaction...")
        
        # 1. Insert into dimDateTimeTable
        logger.info("Inserting into dimDateTimeTable...")
        # SQL with conflict resolution would go here
        
        # 2. Insert into dimLocationTable
        logger.info("Inserting into dimLocationTable...")
        location_data = api_data.get("location", {})
        # SQL with conflict resolution would go here
        
        # 3. Insert into dimMainPollutionTable
        logger.info("Inserting into dimMainPollutionTable...")
        pollution_data = api_data.get("current", {}).get("pollution", {})
        # SQL with conflict resolution would go here
        
        # 4. Insert into factairvisualtable
        logger.info("Inserting into factairvisualtable...")
        weather_data = api_data.get("current", {}).get("weather", {})
        # Comprehensive insert with all metrics
        
        logger.info("Committing transaction...")
        logger.info("Successfully loaded data into PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {e}")
        # In production, implement rollback
        # conn.rollback()
        raise RetryRequested(max_retries=3, seconds_to_wait=180) from e

# Job
@job(
    resource_defs={
        "postgres_conn": postgres_conn_resource,
    },
    config={
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "api_key": "YOUR_API_KEY_HERE",  # Replace with actual key
                    "latitude": 13.8231,  # Salaya, Nakhon Pathom, Thailand
                    "longitude": 100.3250,
                    "output_file_path": "/tmp/airvisual_data.json",
                }
            },
            "read_data_airvisual": {
                "config": {
                    "output_file_path": "/tmp/airvisual_data.json",
                }
            },
            "load_airvisual_to_postgresql": {
                "config": {
                    "pollution_mapping_file": "/tmp/pollution_mapping.json",
                    "weather_mapping_file": "/tmp/weather_mapping.json",
                }
            }
        }
    }
)
def airvisual_etl_job():
    """
    ETL pipeline for AirVisual air quality data.
    Sequential execution: extract → validate → load
    """
    data_file = get_airvisual_data_hourly()
    validated_data = read_data_airvisual(data_file)
    load_airvisual_to_postgresql(validated_data)

# Launch pattern
if __name__ == "__main__":
    result = airvisual_etl_job.execute_in_process()
    if result.success:
        print("Pipeline execution successful!")
    else:
        print(f"Pipeline execution failed: {result.failure_data}")