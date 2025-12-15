import os
import requests
from dagster import op, job, Config, ResourceDefinition, get_dagster_logger

logger = get_dagster_logger()

# Configuration class for pipeline parameters
class SupplierValidationConfig(Config):
    """Configuration for supplier validation pipeline."""
    data_dir: str = os.getenv("DATA_DIR", "/app/data")
    dataset_id: int = 2
    table_name_prefix: str = "JOT_"
    primary_column: str = "supplier_name"
    reconciliator_id: str = "wikidataEntity"
    load_modify_url: str = "http://localhost:3003/load-and-modify"
    reconciliation_url: str = "http://localhost:3003/reconcile"
    save_url: str = "http://localhost:3003/save"
    # Docker-related configs (for documentation)
    docker_network: str = "app_network"
    mongodb_url: str = "mongodb://localhost:27017"
    intertwino_api_url: str = "http://localhost:5005"

# Resource for shared configuration
def supplier_validation_resource(init_context):
    return SupplierValidationConfig()

@op(
    required_resource_keys={"config"},
    retry_policy=RetryPolicy(max_retries=1)
)
def load_and_modify_data(context):
    """
    Step 1: Load supplier CSV, standardize formats, and convert to JSON.
    
    Calls the load-and-modify service (port 3003).
    """
    config = context.resources.config
    input_file = os.path.join(config.data_dir, "suppliers.csv")
    output_file = os.path.join(config.data_dir, "table_data_2.json")
    
    logger.info(f"Loading data from {input_file}")
    
    # Prepare payload for the API call
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config.dataset_id,
        "table_name_prefix": config.table_name_prefix
    }
    
    try:
        response = requests.post(
            config.load_modify_url,
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Successfully created {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Error in load_and_modify_data: {e}")
        raise

@op(
    required_resource_keys={"config"},
    retry_policy=RetryPolicy(max_retries=1)
)
def reconcile_entities(context, json_file_path):
    """
    Step 2: Disambiguate supplier names using Wikidata API.
    
    Uses the reconciliation service (port 3003) to find canonical entities.
    """
    config = context.resources.config
    output_file = os.path.join(config.data_dir, "reconciled_table_2.json")
    
    logger.info(f"Reconciling entities in {json_file_path}")
    
    payload = {
        "input_file": json_file_path,
        "output_file": output_file,
        "primary_column": config.primary_column,
        "reconciliator_id": config.reconciliator_id,
        "dataset_id": config.dataset_id
    }
    
    try:
        response = requests.post(
            config.reconciliation_url,
            json=payload,
            timeout=600
        )
        response.raise_for_status()
        logger.info(f"Successfully created {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Error in reconcile_entities: {e}")
        raise

@op(
    required_resource_keys={"config"},
    retry_policy=RetryPolicy(max_retries=1)
)
def save_final_data(context, reconciled_file_path):
    """
    Step 3: Export validated supplier data to CSV.
    
    Calls the save service to convert JSON back to CSV.
    """
    config = context.resources.config
    output_file = os.path.join(config.data_dir, "enriched_data_2.csv")
    
    logger.info(f"Saving final data from {reconciled_file_path}")
    
    payload = {
        "input_file": reconciled_file_path,
        "output_file": output_file,
        "dataset_id": config.dataset_id
    }
    
    try:
        response = requests.post(
            config.save_url,
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Successfully created {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Error in save_final_data: {e}")
        raise

@job(
    resource_defs={
        "config": ResourceDefinition.hardcoded_resource(
            SupplierValidationConfig(),
            "Shared configuration for supplier validation pipeline"
        )
    }
)
def supplier_validation_pipeline():
    """
    Procurement Supplier Validation Pipeline.
    
    Validates and standardizes supplier data by reconciling names against Wikidata.
    """
    # Define the pipeline flow
    json_data = load_and_modify_data()
    reconciled_data = reconcile_entities(json_data)
    save_final_data(reconciled_data)

if __name__ == "__main__":
    # Execute the pipeline
    result = supplier_validation_pipeline.execute_in_process()
    if result.success:
        print("Pipeline execution completed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")