from dagster import (
    job,
    op,
    Config,
    ResourceParam,
    get_dagster_logger,
    RetryPolicy,
    In,
    Out,
    String,
)
import os
import glob
import json
import requests
from typing import List, Dict, Any

logger = get_dagster_logger()


class DataProcessingConfig(Config):
    """Configuration for the data processing pipeline."""
    data_dir: str = "/app/data"
    dataset_id: int = 2
    date_column: str = "Fecha_id"
    table_prefix: str = "JOT_"
    api_base_url: str = "http://localhost:3003"
    here_api_token: str = "your_here_api_token"  # In production, use secrets
    docker_network: str = "app_network"


class DataProcessingResource:
    """Resource to handle API calls and shared configuration."""
    
    def __init__(self, config: DataProcessingConfig):
        self.config = config
    
    def call_api(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate API call to Docker services."""
        # In real implementation, this would make actual HTTP requests
        logger.info(f"Calling {endpoint} with payload: {payload}")
        # Mock response for demonstration
        return {"status": "success", "message": f"Processed by {endpoint}"}


@op(
    out=Out(List[String]),
    retry_policy=RetryPolicy(max_retries=1),
)
def load_and_modify_data(context, config: DataProcessingConfig, resource: ResourceParam[DataProcessingResource]):
    """
    Load CSV files and convert them to JSON format.
    
    Input: Reads all *.csv files from DATA_DIR
    Output: List of generated JSON file paths
    """
    csv_files = glob.glob(os.path.join(config.data_dir, "*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {config.data_dir}")
    
    output_files = []
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        table_name = f"{config.table_prefix}{filename.replace('.csv', '')}"
        output_file = os.path.join(config.data_dir, f"table_data_{table_name}.json")
        
        # Simulate API call to load-and-modify service
        payload = {
            "input_file": csv_file,
            "output_file": output_file,
            "dataset_id": config.dataset_id,
            "date_column": config.date_column,
            "table_name": table_name,
        }
        
        response = resource.call_api("/load-and-modify", payload)
        
        if response["status"] != "success":
            raise RuntimeError(f"Load and modify failed: {response.get('message')}")
        
        # Simulate file creation
        with open(output_file, 'w') as f:
            json.dump({"source": csv_file, "table": table_name}, f)
        
        output_files.append(output_file)
        logger.info(f"Created {output_file}")
    
    return output_files


@op(
    ins={"input_files": In(List[String])},
    out=Out(List[String]),
    retry_policy=RetryPolicy(max_retries=1),
)
def reconcile_data(context, input_files: List[String], config: DataProcessingConfig, resource: ResourceParam[DataProcessingResource]):
    """
    Standardize and reconcile city names using HERE geocoding service.
    
    Input: table_data_*.json files
    Output: List of reconciled JSON file paths
    """
    output_files = []
    
    for input_file in input_files:
        filename = os.path.basename(input_file)
        output_file = os.path.join(config.data_dir, f"reconciled_{filename}")
        
        # Simulate API call to reconciliation service
        payload = {
            "input_file": input_file,
            "output_file": output_file,
            "primary_column": "City",
            "optional_columns": ["County", "Country"],
            "reconciliator_id": "geocodingHere",
            "api_token": config.here_api_token,
        }
        
        response = resource.call_api("/reconciliation", payload)
        
        if response["status"] != "success":
            raise RuntimeError(f"Reconciliation failed: {response.get('message')}")
        
        # Simulate file creation
        with open(output_file, 'w') as f:
            json.dump({"reconciled": True, "source": input_file}, f)
        
        output_files.append(output_file)
        logger.info(f"Created {output_file}")
    
    return output_files


@op(
    ins={"input_files": In(List[String])},
    out=Out(List[String]),
    retry_policy=RetryPolicy(max_retries=1),
)
def extend_with_openmeteo(context, input_files: List[String], config: DataProcessingConfig, resource: ResourceParam[DataProcessingResource]):
    """
    Enrich dataset with weather information from OpenMeteo.
    
    Input: reconciled_table_*.json files
    Output: List of OpenMeteo enriched JSON file paths
    """
    output_files = []
    
    for input_file in input_files:
        filename = os.path.basename(input_file)
        output_file = os.path.join(config.data_dir, f"open_meteo_{filename}")
        
        # Simulate API call to OpenMeteo extension service
        payload = {
            "input_file": input_file,
            "output_file": output_file,
            "weather_attributes": ["apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "precipitation_hours"],
            "date_format_separator": "-",  # Configurable
        }
        
        response = resource.call_api("/openmeteo-extension", payload)
        
        if response["status"] != "success":
            raise RuntimeError(f"OpenMeteo extension failed: {response.get('message')}")
        
        # Simulate file creation
        with open(output_file, 'w') as f:
            json.dump({"weather_enriched": True, "source": input_file}, f)
        
        output_files.append(output_file)
        logger.info(f"Created {output_file}")
    
    return output_files


@op(
    ins={"input_files": In(List[String])},
    out=Out(List[String]),
    retry_policy=RetryPolicy(max_retries=1),
)
def extend_columns(context, input_files: List[String], config: DataProcessingConfig, resource: ResourceParam[DataProcessingResource]):
    """
    Append additional data properties to the dataset.
    
    Input: open_meteo_*.json files
    Output: List of column extended JSON file paths
    """
    output_files = []
    
    for input_file in input_files:
        filename = os.path.basename(input_file)
        output_file = os.path.join(config.data_dir, f"column_extended_{filename}")
        
        # Simulate API call to column extension service
        payload = {
            "input_file": input_file,
            "output_file": output_file,
            "extender_id": "reconciledColumnExt",
            "additional_properties": ["id", "name"],  # As per description
        }
        
        response = resource.call_api("/column-extension", payload)
        
        if response["status"] != "success":
            raise RuntimeError(f"Column extension failed: {response.get('message')}")
        
        # Simulate file creation
        with open(output_file, 'w') as f:
            json.dump({"columns_extended": True, "source": input_file}, f)
        
        output_files.append(output_file)
        logger.info(f"Created {output_file}")
    
    return output_files


@op(
    ins={"input_files": In(List[String])},
    retry_policy=RetryPolicy(max_retries=1),
)
def save_final_data(context, input_files: List[String], config: DataProcessingConfig, resource: ResourceParam[DataProcessingResource]):
    """
    Consolidate and export the fully enriched dataset as CSV.
    
    Input: column_extended_*.json files
    Output: Final CSV file paths
    """
    output_files = []
    
    for input_file in input_files:
        filename = os.path.basename(input_file)
        # Extract table name from the filename pattern
        table_name = filename.replace("column_extended_open_meteo_reconciled_table_data_", "").replace(".json", "")
        output_file = os.path.join(config.data_dir, f"enriched_data_{table_name}.csv")
        
        # Simulate API call to save service
        payload = {
            "input_file": input_file,
            "output_file": output_file,
        }
        
        response = resource.call_api("/save", payload)
        
        if response["status"] != "success":
            raise RuntimeError(f"Save failed: {response.get('message')}")
        
        # Simulate file creation
        with open(output_file, 'w') as f:
            f.write("enriched_data\n")
        
        output_files.append(output_file)
        logger.info(f"Created final output {output_file}")
    
    return output_files


@job
def data_enrichment_job():
    """
    End-to-end data enrichment pipeline.
    
    Sequential steps:
    1. Load and modify CSV data
    2. Reconcile city names
    3. Extend with OpenMeteo weather data
    4. Extend with additional columns
    5. Save final enriched CSV
    """
    config = DataProcessingConfig()
    resource = DataProcessingResource(config)
    
    # Chain operations sequentially
    modified_data = load_and_modify_data(config, resource)
    reconciled_data = reconcile_data(modified_data, config, resource)
    weather_data = extend_with_openmeteo(reconciled_data, config, resource)
    extended_data = extend_columns(weather_data, config, resource)
    save_final_data(extended_data, config, resource)


if __name__ == "__main__":
    # Execute the job in-process for testing
    result = data_enrichment_job.execute_in_process()
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")