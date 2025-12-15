import os
import json
import requests
from dagster import (
    op,
    job,
    ConfigurableResource,
    RetryPolicy,
    OpExecutionContext,
    get_dagster_logger,
)
from pydantic import Field
from typing import Dict, Any

logger = get_dagster_logger()

class ProcurementConfig(ConfigurableResource):
    """Configuration for the procurement supplier validation pipeline."""
    data_dir: str = Field(default=os.getenv("DATA_DIR", "./data"))
    api_base_url: str = Field(default="http://localhost:3003")
    dataset_id: int = Field(default=2)
    table_name_prefix: str = Field(default="JOT_")
    primary_column: str = Field(default="supplier_name")
    reconciliator_id: str = Field(default="wikidataEntity")

    def get_input_path(self, filename: str) -> str:
        return os.path.join(self.data_dir, filename)
    
    def get_output_path(self, filename: str) -> str:
        return os.path.join(self.data_dir, filename)


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Load supplier CSV, standardize formats, and convert to JSON"
)
def load_and_modify_data(context: OpExecutionContext, config: ProcurementConfig) -> str:
    """Calls the load-and-modify service to process supplier data."""
    input_file = config.get_input_path("suppliers.csv")
    output_file = config.get_output_path("table_data_2.json")
    
    # Verify input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Prepare payload for the service
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config.dataset_id,
        "table_name_prefix": config.table_name_prefix,
    }
    
    try:
        # Call the load-and-modify service
        response = requests.post(
            f"{config.api_base_url}/load-and-modify",
            json=payload,
            timeout=300  # 5 minute timeout
        )
        response.raise_for_status()
        
        result = response.json()
        if not result.get("success"):
            raise Exception(f"Service failed: {result.get('error', 'Unknown error')}")
        
        logger.info(f"Successfully processed data to {output_file}")
        return output_file
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call load-and-modify service: {e}")
        raise


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Disambiguate supplier names using Wikidata API"
)
def reconcile_entities(
    context: OpExecutionContext, 
    config: ProcurementConfig, 
    input_file: str
) -> str:
    """Calls the reconciliation service to find canonical Wikidata entities."""
    output_file = config.get_output_path("reconciled_table_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "primary_column": config.primary_column,
        "reconciliator_id": config.reconciliator_id,
        "dataset_id": config.dataset_id,
    }
    
    try:
        response = requests.post(
            f"{config.api_base_url}/reconciliation",
            json=payload,
            timeout=600  # 10 minute timeout
        )
        response.raise_for_status()
        
        result = response.json()
        if not result.get("success"):
            raise Exception(f"Reconciliation failed: {result.get('error', 'Unknown error')}")
        
        logger.info(f"Successfully reconciled entities to {output_file}")
        return output_file
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call reconciliation service: {e}")
        raise


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Export validated supplier data to CSV"
)
def save_final_data(
    context: OpExecutionContext, 
    config: ProcurementConfig, 
    input_file: str
) -> str:
    """Calls the save service to export data to CSV format."""
    output_file = config.get_output_path("enriched_data_2.csv")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config.dataset_id,
    }
    
    try:
        response = requests.post(
            f"{config.api_base_url}/save",
            json=payload,
            timeout=300  # 5 minute timeout
        )
        response.raise_for_status()
        
        result = response.json()
        if not result.get("success"):
            raise Exception(f"Save failed: {result.get('error', 'Unknown error')}")
        
        logger.info(f"Successfully saved final data to {output_file}")
        return output_file
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call save service: {e}")
        raise


@job(
    description="Validates and standardizes supplier data by reconciling names against Wikidata"
)
def procurement_supplier_validation_job():
    """Defines the procurement supplier validation pipeline."""
    config = ProcurementConfig()
    
    # Chain the operations in sequence
    reconciled_file = reconcile_entities(
        config,
        load_and_modify_data(config)
    )
    
    save_final_data(config, reconciled_file)


if __name__ == "__main__":
    # Ensure data directory exists
    data_dir = os.getenv("DATA_DIR", "./data")
    os.makedirs(data_dir, exist_ok=True)
    
    # Execute the job
    result = procurement_supplier_validation_job.execute_in_process()
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")