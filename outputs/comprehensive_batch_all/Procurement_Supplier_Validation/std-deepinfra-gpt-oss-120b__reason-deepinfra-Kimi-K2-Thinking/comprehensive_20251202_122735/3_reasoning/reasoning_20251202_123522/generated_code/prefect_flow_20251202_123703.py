import os
import subprocess
from pathlib import Path
from typing import Optional

from prefect import flow, task

# Configuration from environment variables
DATA_DIR = os.getenv("DATA_DIR", "/data")
DATASET_ID = os.getenv("DATASET_ID", "2")
TABLE_NAME_PREFIX = os.getenv("TABLE_NAME_PREFIX", "JOT_")
PRIMARY_COLUMN = os.getenv("PRIMARY_COLUMN", "supplier_name")
RECONCILIATOR_ID = os.getenv("RECONCILIATOR_ID", "wikidataEntity")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "app_network")


@task(retries=1)
def load_and_modify_data(
    data_dir: str,
    dataset_id: str,
    table_name_prefix: str
) -> str:
    """Load supplier CSV, standardize formats, and convert to JSON."""
    input_file = Path(data_dir) / "suppliers.csv"
    output_file = Path(data_dir) / f"table_data_{dataset_id}.json"

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    cmd = [
        "docker", "run",
        "--network", DOCKER_NETWORK,
        "-v", f"{data_dir}:{data_dir}",
        "-e", f"DATASET_ID={dataset_id}",
        "-e", f"TABLE_NAME_PREFIX={table_name_prefix}",
        "-e", f"INPUT_FILE={input_file}",
        "-e", f"OUTPUT_FILE={output_file}",
        "--rm",
        "i2t-backendwithintertwino6-load-and-modify:latest",
    ]

    subprocess.run(cmd, capture_output=True, text=True, check=True)
    return str(output_file)


@task(retries=1)
def reconcile_entities(
    input_file: str,
    dataset_id: str,
    primary_column: str,
    reconciliator_id: str
) -> str:
    """Disambiguate supplier names using Wikidata API."""
    data_dir = Path(input_file).parent
    output_file = data_dir / f"reconciled_table_{dataset_id}.json"

    cmd = [
        "docker", "run",
        "--network", DOCKER_NETWORK,
        "-v", f"{data_dir}:{data_dir}",
        "-e", f"INPUT_FILE={input_file}",
        "-e", f"OUTPUT_FILE={output_file}",
        "-e", f"PRIMARY_COLUMN={primary_column}",
        "-e", f"RECONCILIATOR_ID={reconciliator_id}",
        "-e", f"DATASET_ID={dataset_id}",
        "--rm",
        "i2t-backendwithintertwino6-reconciliation:latest",
    ]

    subprocess.run(cmd, capture_output=True, text=True, check=True)
    return str(output_file)


@task(retries=1)
def save_final_data(input_file: str, dataset_id: str) -> str:
    """Export validated supplier data to CSV."""
    data_dir = Path(input_file).parent
    output_file = data_dir / f"enriched_data_{dataset_id}.csv"

    cmd = [
        "docker", "run",
        "--network", DOCKER_NETWORK,
        "-v", f"{data_dir}:{data_dir}",
        "-e", f"INPUT_FILE={input_file}",
        "-e", f"OUTPUT_FILE={output_file}",
        "-e", f"DATASET_ID={dataset_id}",
        "--rm",
        "i2t-backendwithintertwino6-save:latest",
    ]

    subprocess.run(cmd, capture_output=True, text=True, check=True)
    return str(output_file)


@flow
def procurement_supplier_validation_pipeline(
    data_dir: Optional[str] = None,
    dataset_id: str = DATASET_ID,
    table_name_prefix: str = TABLE_NAME_PREFIX,
    primary_column: str = PRIMARY_COLUMN,
    reconciliator_id: str = RECONCILIATOR_ID,
) -> str:
    """Main flow for supplier data validation and standardization.

    Steps:
    1. Load & Modify Data: Convert CSV to standardized JSON
    2. Entity Reconciliation: Disambiguate names against Wikidata
    3. Save Final Data: Export reconciled data to CSV

    Note: For scheduled execution, deploy with Prefect Cloud or Server
    and configure a schedule in the deployment.
    """
    data_dir = data_dir or DATA_DIR

    # Step 1: Load and modify supplier data
    json_file = load_and_modify_data(
        data_dir=data_dir,
        dataset_id=dataset_id,
        table_name_prefix=table_name_prefix,
    )

    # Step 2: Reconcile entities against Wikidata
    reconciled_file = reconcile_entities(
        input_file=json_file,
        dataset_id=dataset_id,
        primary_column=primary_column,
        reconciliator_id=reconciliator_id,
    )

    # Step 3: Save final enriched data
    final_csv = save_final_data(
        input_file=reconciled_file,
        dataset_id=dataset_id,
    )

    return final_csv


if __name__ == "__main__":
    # Execute flow locally
    procurement_supplier_validation_pipeline()