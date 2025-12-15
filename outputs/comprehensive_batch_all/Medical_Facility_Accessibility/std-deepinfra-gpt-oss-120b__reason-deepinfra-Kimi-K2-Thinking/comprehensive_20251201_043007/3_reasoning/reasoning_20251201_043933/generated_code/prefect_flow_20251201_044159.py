import os
import json
import csv
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, task


@task(
    name="load_and_modify_data",
    retries=1,
    retry_delay_seconds=30,
    description="Ingest facility CSV, clean addresses, convert to JSON"
)
def load_and_modify_data(
    data_dir: str,
    dataset_id: int,
    table_name_prefix: str
) -> str:
    """Load facilities.csv, clean and standardize addresses, convert to JSON."""
    input_path = Path(data_dir) / "facilities.csv"
    output_path = Path(data_dir) / f"table_data_{dataset_id}.json"
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Simulate API call to load-and-modify service
    # Docker: i2t-backendwithintertwino6-load-and-modify:latest
    # In production: requests.post("http://localhost:3003/load-and-modify", ...)
    
    mock_data = {
        "facilities": [
            {"id": 1, "name": "Hospital A", "address": "123 Main St"},
            {"id": 2, "name": "Clinic B", "address": "456 Oak Ave"}
        ],
        "dataset_id": dataset_id,
        "table_name_prefix": table_name_prefix
    }
    
    with open(output_path, 'w') as f:
        json.dump(mock_data, f, indent=2)
    
    return str(output_path)


@task(
    name="reconcile_geocoding",
    retries=1,
    retry_delay_seconds=30,
    description="Geocode facility locations using HERE API"
)
def reconcile_geocoding(
    data_dir: str,
    dataset_id: int,
    primary_column: str,
    reconciliator_id: str,
    api_token: str,
    input_file: str
) -> str:
    """Geocode facility locations using HERE API."""
    output_path = Path(data_dir) / f"reconciled_table_{dataset_id}.json"
    
    # Simulate API call to reconciliation service
    # Docker: i2t-backendwithintertwino6-reconciliation:latest
    # In production: requests.post("http://localhost:3003/reconcile", ...)
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    for i, facility in enumerate(data.get("facilities", [])):
        facility["latitude"] = 40.7128 + i * 0.01
        facility["longitude"] = -74.0060 + i * 0.01
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return str(output_path)


@task(
    name="calculate_distance_public_transport",
    retries=1,
    retry_delay_seconds=30,
    description="Calculate distance to nearest public transport"
)
def calculate_distance_public_transport(
    data_dir: str,
    dataset_id: int,
    extender_id: str,
    lat_column: str,
    lon_column: str,
    target_layer: str,
    target_data_source: str,
    output_column: str,
    input_file: str
) -> str:
    """Calculate distance from each facility to nearest public transport."""
    output_path = Path(data_dir) / f"distance_pt_{dataset_id}.json"
    
    # Simulate API call to column-extension service
    # Docker: i2t-backendwithintertwino6-column-extension:latest
    # In production: requests.post("http://localhost:3003/column-extension", ...)
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    for i, facility in enumerate(data.get("facilities", [])):
        facility[output_column] = 0.5 + i * 0.1
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return str(output