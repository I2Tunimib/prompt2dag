import os
import glob
import json
from pathlib import Path
from typing import Dict, Any, Optional

import httpx
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner  # Since it's strictly sequential

# Configuration from environment
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "app_network")
HERE_API_TOKEN = os.getenv("HERE_API_TOKEN", "")
DATASET_ID = int(os.getenv("DATASET_ID", "2"))
DATE_COLUMN = os.getenv("DATE_COLUMN", "Fecha_id")

# API endpoints
LOAD_MODIFY_URL = "http://load-and-modify:3003/load-and-modify"
RECONCILIATION_URL = "http://reconciliation:3003/reconcile"
OPENMETEO_URL = "http://openmeteo-extension:3003/extend"
COLUMN_EXT_URL = "http://column-extension:3003/extend"
SAVE_URL = "http://save:3003/save"

@task(retries=1, retry_delay_seconds=30)
def load_and_modify_data(data_dir: str, dataset_id: int, date_column: str) -> str:
    """Load CSV files and convert to JSON format."""
    logger = get_run_logger()
    logger.info(f"Loading CSV files from {data_dir}")
    
    # Find all CSV files
    csv_files = glob.glob(f"{data_dir}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    output_file = f"{data_dir}/table_data_{dataset_id}.json"
    
    # Simulate API call to load-and-modify service
    payload = {
        "csv_files": csv_files,
        "dataset_id": dataset_id,
        "date_column": date_column,
        "table_naming": f"JOT_{dataset_id}"
    }
    
    try:
        response = httpx.post(LOAD_MODIFY_URL, json=payload, timeout=300)
        response.raise_for_status()
        
        # Save the result
        result = response.json()
        with open(output_file, 'w') as f:
            json.dump(result, f)
            
        logger.info(f"Successfully created {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Failed to load and modify data: {e}")
        raise

@task(retries=1, retry_delay_seconds=30)
def reconcile_data(input_file: str, dataset_id: int, api_token: str) -> str:
    """Standardize and reconcile city names using HERE geocoding."""
    logger = get_run_logger()
    logger.info(f"Reconciling data from {input_file}")
    
    data_dir = Path(input_file).parent
    output_file = f"{data_dir}/reconciled_table_{dataset_id}.json"
    
    # Read input data
    with open(input_file, 'r') as f:
        input_data = json.load(f)
    
    # API call to reconciliation service
    payload = {
        "data": input_data,
        "primary_column": "City",
        "optional_columns": ["County", "Country"],
        "reconciliator_id": "geocodingHere",
        "api_token": api_token
    }
    
    try:
        response = httpx.post(RECONCILIATION_URL, json=payload, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        with open(output_file, 'w') as f:
            json.dump(result, f)
            
        logger.info(f"Successfully created {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Failed to reconcile data: {e}")
        raise

@task(retries=1, retry_delay_seconds=30)
def extend_with_openmeteo(input_file: str, dataset_id: int) -> str:
    """Enrich dataset with weather information from OpenMeteo."""
    logger = get_run_logger()
    logger.info(f"Extending data with OpenMeteo from {input_file}")
    
    data_dir = Path(input_file).parent
    output_file = f"{data_dir}/open_meteo_{dataset_id}.json"
    
    # Read input data
    with open(input_file, 'r') as f:
        input_data = json.load(f)
    
    # API call to OpenMeteo extension service
    payload = {
        "data": input_data,
        "weather_attributes": {
            "apparent_temperature_max": True,
            "apparent_temperature_min": True,
            "precipitation_sum": True,
            "precipitation_hours": True
        },
        "date_format_separator": "-"  # Configurable
    }
    
    try:
        response = httpx.post(OPENMETEO_URL, json=payload, timeout=600)
        response.raise_for_status()
        
        result = response.json()
        with open(output_file, 'w') as f:
            json.dump(result, f)
            
        logger.info(f"Successfully created {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Failed to extend with OpenMeteo: {e}")
        raise

@task(retries=1, retry_delay_seconds=30)
def extend_columns(input_file: str, dataset_id: int) -> str:
    """Append additional data properties."""
    logger = get_run_logger()
    logger.info(f"Extending columns from {input_file}")
    
    data_dir = Path(input_file).parent
    output_file = f"{data_dir}/column_extended_{dataset_id}.json"
    
    # Read input data
    with open(input_file, 'r') as f:
        input_data = json.load(f)
    
    # API call to column extension service
    payload = {
        "data": input_data,
        "extender_id": "reconciledColumnExt",
        "properties": ["id", "name"]  # As per description
    }
    
    try:
        response = httpx.post(COLUMN_EXT_URL, json=payload, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        with open(output_file, 'w') as f:
            json.dump(result, f)
            
        logger.info(f"Successfully created {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Failed to extend columns: {e}")
        raise

@task(retries=1, retry_delay_seconds=30)
def save_final_data(input_file: str, dataset_id: int, data_dir: str) -> str:
    """Consolidate and export the fully enriched dataset as CSV."""
    logger = get_run_logger()
    logger.info(f"Saving final data from {input_file}")
    
    output_file = f"{data_dir}/enriched_data_{dataset_id}.csv"
    
    # Read input data
    with open(input_file, 'r') as f:
        input_data = json.load(f)
    
    # API call to save service
    payload = {
        "data": input_data,
        "output_format": "csv",
        "output_path": output_file
    }
    
    try:
        response = httpx.post(SAVE_URL, json=payload, timeout=300)
        response.raise_for_status()
        
        logger.info(f"Successfully saved {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Failed to save final data: {e}")
        raise

@flow(name="data-enrichment-pipeline", task_runner=SequentialTaskRunner())
def data_enrichment_pipeline(
    data_dir: Optional[str] = None,
    dataset_id: Optional[int] = None,
    date_column: Optional[str] = None,
    here_api_token: Optional[str] = None
):
    """
    End-to-end data enrichment pipeline.
    
    Args:
        data_dir: Directory containing input CSV files and where outputs are saved
        dataset_id: Dataset identifier for file naming
        date_column: Name of the date column in source data
        here_api_token: API token for HERE geocoding service
    """
    logger = get_run_logger()
    
    # Resolve parameters with environment variable fallbacks
    data_dir = data_dir or DATA_DIR
    dataset_id = dataset_id or DATASET_ID
    date_column = date_column or DATE_COLUMN
    here_api_token = here_api_token or HERE_API_TOKEN
    
    logger.info(f"Starting data enrichment pipeline for dataset {dataset_id}")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Date column: {date_column}")
    
    # Step 1: Load and Modify Data
    table_json = load_and_modify_data(
        data_dir=data_dir,
        dataset_id=dataset_id,
        date_column=date_column
    )
    
    # Step 2: Data Reconciliation
    reconciled_json = reconcile_data(
        input_file=table_json,
        dataset_id=dataset_id,
        api_token=here_api_token
    )
    
    # Step 3: OpenMeteo Data Extension
    openmeteo_json = extend_with_openmeteo(
        input_file=reconciled_json,
        dataset_id=dataset_id
    )
    
    # Step 4: Column Extension
    column_extended_json = extend_columns(
        input_file=openmeteo_json,
        dataset_id=dataset_id
    )
    
    # Step 5: Save Final Data
    final_csv = save_final_data(
        input_file=column_extended_json,
        dataset_id=dataset_id,
        data_dir=data_dir
    )
    
    logger.info(f"Pipeline completed successfully. Final output: {final_csv}")
    return final_csv

if __name__ == "__main__":
    # For local execution
    data_enrichment_pipeline()