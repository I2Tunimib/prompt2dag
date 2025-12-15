import os
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner


def run_docker_container(
    image: str,
    data_dir: str,
    environment: Optional[Dict[str, Any]] = None,
    network: str = "app_network"
) -> str:
    """Helper to run a docker container with standard configuration."""
    logger = get_run_logger()
    
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{data_dir}:/app/data",
        "--network", network,
    ]
    
    if environment:
        for key, value in environment.items():
            cmd.extend(["-e", f"{key}={value}"])
    
    cmd.append(image)
    
    logger.info(f"Running Docker container: {image}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False
    )
    
    if result.stdout:
        logger.debug(f"Container output: {result.stdout}")
    
    if result.returncode != 0:
        logger.error(f"Container error: {result.stderr}")
        raise RuntimeError(
            f"Docker container '{image}' failed with exit code {result.returncode}"
        )
    
    return result.stdout


@task(
    name="load_and_modify_data",
    retries=1,
    retry_delay_seconds=30,
    description="Ingest CSV files and convert to JSON format"
)
def load_and_modify_data(
    data_dir: str,
    dataset_id: int = 2,
    date_column: str = "Fecha_id"
) -> int:
    """
    Load and modify CSV data by calling the load-and-modify service.
    
    - Input: DATA_DIR/*.csv
    - Output: table_data_*.json
    - Docker Image: i2t-backendwithintertwino6-load-and-modify:latest
    """
    logger = get_run_logger()
    logger.info("Starting load and modify step")
    
    image = "i2t-backendwithintertwino6-load-and-modify:latest"
    
    environment = {
        "DATA_DIR": "/app/data",
        "DATASET_ID": str(dataset_id),
        "DATE_COLUMN": date_column,
        "TABLE_NAME_PREFIX": "JOT_"
    }
    
    run_docker_container(image, data_dir, environment)
    
    output_files = list(Path(data_dir).glob("table_data_*.json"))
    if not output_files:
        raise FileNotFoundError("No table_data_*.json files generated")
    
    logger.info(f"Generated {len(output_files)} JSON files")
    return len(output_files)


@task(
    name="reconcile_data",
    retries=1,
    retry_delay_seconds=30,
    description="Standardize city names using HERE geocoding service"
)
def reconcile_data(
    data_dir: str,
    api_token: str
) -> int:
    """
    Reconcile city names using the reconciliation service.
    
    - Input: table_data_*.json
    - Output: reconciled_table_*.json
    - Docker Image: i2t-backendwithintertwino6-reconciliation:latest
    """
    logger = get_run_logger()
    logger.info("Starting data reconciliation step")
    
    if not api_token:
        raise ValueError("API token is required for reconciliation")
    
    image = "i2t-backendwithintertwino6-reconciliation:latest"
    
    environment = {
        "DATA_DIR": "/app/data",
        "API_TOKEN": api_token,
        "PRIMARY_COLUMN": "City",
        "OPTIONAL_COLUMNS": "County,Country",
        "RECONCILIATOR_ID": "geocodingHere"
    }
    
    run_docker_container(image, data_dir, environment)
    
    output_files = list(Path(data_dir).glob("reconciled_table_*.json"))
    if not output_files:
        raise FileNotFoundError("No reconciled_table_*.json files generated")
    
    logger.info(f"Generated {len(output_files)} reconciled files")
    return len(output_files)


@task(
    name="extend_with_openmeteo",
    retries=1,
    retry_delay_seconds=30,
    description