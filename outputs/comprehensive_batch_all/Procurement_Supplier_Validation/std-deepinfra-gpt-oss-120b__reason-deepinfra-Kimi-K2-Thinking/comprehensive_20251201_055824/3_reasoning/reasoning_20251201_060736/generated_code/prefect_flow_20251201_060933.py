import os
from pathlib import Path
from typing import Dict, Any
import requests
from prefect import flow, task, get_run_logger

# Configuration from environment variables
DATA_DIR = os.getenv("DATA_DIR", "/data")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:3003")
SAVE_SERVICE_URL = os.getenv("SAVE_SERVICE_URL", "http://localhost:3003")


@task(retries=1, retry_delay_seconds=30)
def load_and_modify_data() -> str:
    """Load supplier CSV, standardize formats, and convert to JSON."""
    logger = get_run_logger()
    input_path = Path(DATA_DIR) / "suppliers.csv"
    output_path = Path(DATA_DIR) / "table_data_2.json"
    
    logger.info(f"Processing {input_path} -> {output_path}")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Call load-and-modify service
    # Docker: docker run --rm -v $DATA_DIR:/data --network app_network
    #   -e DATASET_ID=2 -e TABLE_NAME_PREFIX=JOT_
    #   i2t-backendwithintertwino6-load-and-modify:latest
    response = requests.post(
        f"{API_BASE_URL}/load-and-modify",
        json={
            "input_path": str(input_path),
            "output_path": str(output_path),
            "dataset_id": 2,
            "table_name_prefix": "JOT_"
        },
        timeout=300
    )
    response.raise_for_status()
    
    if not output_path.exists():
        raise FileNotFoundError(f"Output not created: {output_path}")
    
    logger.info(f"Created {output_path}")
    return str(output_path)


@task(retries=1, retry_delay_seconds=30)
def reconcile_entities(input_json_path: str) -> str:
    """Disambiguate supplier names using Wikidata reconciliation."""
    logger = get_run_logger()
    input_path = Path(input_json_path)
    output_path = Path(DATA_DIR) / "reconciled_table_2.json"
    
    logger.info(f"Reconciling {input_path}")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Call reconciliation service
    # Docker: docker run --rm -v $DATA_DIR:/data --network app_network
    #   -e PRIMARY_COLUMN=supplier_name -e RECONCILIATOR_ID=wikidataEntity
    #   -e DATASET_ID=2
    #   i2t-backendwithintertwino6-reconciliation:latest
    response = requests.post(
        f"{API_BASE_URL}/reconciliation",
        json={
            "input_path": str(input_path),
            "output_path": str(output_path),
            "primary_column": "supplier_name",
            "reconciliator_id": "wikidataEntity",
            "dataset_id": 2
        },
        timeout=300
    )
    response.raise_for_status()
    
    if not output_path.exists():
        raise FileNotFoundError(f"Output not created: {output_path}")
    
    logger.info(f"Created {output_path}")
    return str(output_path)


@task(retries=1, retry_delay_seconds=30)
def save_final_data(input_json_path: str) -> str:
    """Export validated supplier data to final CSV format."""
    logger = get_run_logger()
    input_path = Path(input_json_path)
    output_path = Path(DATA_DIR) / "enriched_data_2.csv"
    
    logger.info(f"Saving {input_path} -> {output_path}")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Call save service
    # Docker: docker run --rm -v $DATA_DIR:/data --network app_network
    #   -e DATASET_ID=2
    #   i2t-backendwithintertwino6-save:latest
    response = requests.post(
        f"{SAVE_SERVICE_URL}/save",
        json={
            "input_path": str(input_path),
            "output_path": str(output_path),
            "dataset_id": 2
        },
        timeout=300
    )
    response.raise_for_status()
    
    if not output_path.exists():
        raise FileNotFoundError(f"Output not created: {output_path}")
    
    logger.info(f"Created {output_path}")
    return str(output_path)


@flow(name="procurement-supplier-validation")
def procurement_supplier_validation_flow() -> Dict[str, Any]:
    """Orchestrate supplier data validation pipeline.
    
    Steps execute sequentially:
    1. Load and modify supplier CSV data
    2. Reconcile entities against Wikidata
    3. Save enriched data to CSV
    """
    logger = get_run_logger()
    logger.info("Starting procurement supplier validation pipeline")
    
    # Step 1: Load & Modify Data
    modified_data_path = load_and_modify_data()
    
    # Step 2: Entity Reconciliation
    reconciled_data_path = reconcile_entities(modified_data_path)
    
    # Step 3: Save Final Data
    final_output_path = save_final_data(reconciled_data_path)
    
    logger.info(f"Pipeline completed: {final_output_path}")
    
    return {
        "status": "success",
        "final_output_path": final_output_path
    }


if __name__ == "__main__":
    # Local execution entry point
    # Prerequisites:
    # - DATA_DIR environment variable set
    # - API services running on configured ports
    # - suppliers.csv exists in DATA_DIR
    
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    procurement_supplier_validation_flow()