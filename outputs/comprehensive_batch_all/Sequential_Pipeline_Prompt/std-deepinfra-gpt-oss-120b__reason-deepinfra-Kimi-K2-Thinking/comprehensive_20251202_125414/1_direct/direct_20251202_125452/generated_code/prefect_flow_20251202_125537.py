import os
import subprocess
from pathlib import Path
from typing import List, Optional

from prefect import flow, task


def _run_docker(
    image: str,
    env: Optional[dict] = None,
    data_dir: Optional[str] = None,
    command: Optional[List[str]] = None,
) -> None:
    """
    Execute a Docker container with the given configuration.

    Args:
        image: Docker image to run.
        env: Environment variables to set inside the container.
        data_dir: Host directory to mount at ``/app/data`` inside the container.
        command: Optional command arguments passed to the container.
    """
    cmd = ["docker", "run", "--rm", "--network", "app_network"]
    if data_dir:
        host_path = os.path.abspath(data_dir)
        cmd.extend(["-v", f"{host_path}:/app/data"])
    if env:
        for key, value in env.items():
            cmd.extend(["-e", f"{key}={value}"])
    cmd.append(image)
    if command:
        cmd.extend(command)

    subprocess.run(cmd, check=True)


@task
def load_and_modify(data_dir: str, dataset_id: int = 2, date_column: str = "Fecha_id") -> List[Path]:
    """
    Ingest CSV files and produce JSON files using the load‑and‑modify service.
    """
    env = {
        "DATA_DIR": "/app/data",
        "DATASET_ID": str(dataset_id),
        "DATE_COLUMN": date_column,
        "TABLE_PREFIX": "JOT_",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        env=env,
        data_dir=data_dir,
    )
    return sorted(Path(data_dir).glob("table_data_*.json"))


@task
def data_reconciliation(data_dir: str, previous_files: List[Path]) -> List[Path]:
    """
    Standardize city names using the HERE geocoding service.
    """
    env = {
        "DATA_DIR": "/app/data",
        "RECONCILIATOR_ID": "geocodingHere",
        "PRIMARY_COLUMN": "City",
        "OPTIONAL_COLUMNS": "County,Country",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-reconciliation:latest",
        env=env,
        data_dir=data_dir,
    )
    return sorted(Path(data_dir).glob("reconciled_table_*.json"))


@task
def openmeteo_extension(data_dir: str, previous_files: List[Path]) -> List[Path]:
    """
    Enrich the dataset with weather information from OpenMeteo.
    """
    env = {
        "DATA_DIR": "/app/data",
        "WEATHER_ATTRIBUTES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        env=env,
        data_dir=data_dir,
    )
    return sorted(Path(data_dir).glob("open_meteo_*.json"))


@task
def column_extension(data_dir: str, previous_files: List[Path]) -> List[Path]:
    """
    Append additional data properties using the column‑extension service.
    """
    env = {
        "DATA_DIR": "/app/data",
        "EXTENDER_ID": "reconciledColumnExt",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-column-extension:latest",
        env=env,
        data_dir=data_dir,
    )
    return sorted(Path(data_dir).glob("column_extended_*.json"))


@task
def save_final_data(data_dir: str, previous_files: List[Path]) -> Path:
    """
    Consolidate and export the fully enriched dataset as a CSV file.
    """
    env = {
        "DATA_DIR": "/app/data",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-save:latest",
        env=env,
        data_dir=data_dir,
    )
    csv_files = list(Path(data_dir).glob("enriched_data_*.csv"))
    if not csv_files:
        raise FileNotFoundError("No enriched CSV file was produced by the save step.")
    return csv_files[0]


@flow
def data_processing_flow() -> Path:
    """
    Orchestrates the end‑to‑end data processing pipeline.
    """
    data_dir = os.getenv("DATA_DIR", "./data")
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    step1 = load_and_modify(data_dir)
    step2 = data_reconciliation(data_dir, step1)
    step3 = openmeteo_extension(data_dir, step2)
    step4 = column_extension(data_dir, step3)
    final_csv = save_final_data(data_dir, step4)

    return final_csv


# Deployment configuration (e.g., schedule) can be added via Prefect UI or programmatic
# deployment scripts; this module provides the flow definition only.

if __name__ == "__main__":
    result_path = data_processing_flow()
    print(f"Pipeline completed. Final CSV located at: {result_path}")