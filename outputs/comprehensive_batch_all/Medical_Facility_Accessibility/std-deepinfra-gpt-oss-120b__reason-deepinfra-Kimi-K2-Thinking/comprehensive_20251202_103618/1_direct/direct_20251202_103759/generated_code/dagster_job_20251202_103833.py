import os
import json
import csv
from pathlib import Path
from typing import List, Dict

from dagster import op, job, In, Out, Nothing, String, get_dagster_logger


# Default data directory; can be overridden by the DATA_DIR environment variable.
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _read_csv(file_path: Path) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dictionaries."""
    with file_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


def _write_json(data: List[Dict], file_path: Path) -> None:
    """Write a list of dictionaries to a JSON file."""
    with file_path.open("w", encoding="utf-8") as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=2)


def _read_json(file_path: Path) -> List[Dict]:
    """Read a JSON file containing a list of dictionaries."""
    with file_path.open("r", encoding="utf-8") as jsonfile:
        return json.load(jsonfile)


def _write_csv(data: List[Dict], file_path: Path) -> None:
    """Write a list of dictionaries to a CSV file."""
    if not data:
        return
    with file_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


@op(out=Out(String))
def load_and_modify_data() -> str:
    """
    Load facilities CSV, clean address field, and write JSON.
    Returns the path to the generated JSON file.
    """
    logger = get_dagster_logger()
    input_path = DATA_DIR / "facilities.csv"
    output_path = DATA_DIR / "table_data_2.json"

    logger.info(f"Loading CSV from {input_path}")
    rows = _read_csv(input_path)

    # Simple cleaning: strip whitespace from address field if present.
    for row in rows:
        if "address" in row and isinstance(row["address"], str):
            row["address"] = row["address"].strip()

    logger.info(f"Writing cleaned data to JSON at {output_path}")
    _write_json(rows, output_path)

    return str(output_path)


@op(ins={"json_path": In(String)}, out=Out(String))
def reconcile_geocode(json_path: str) -> str:
    """
    Simulate geocoding by adding dummy latitude and longitude fields.
    Returns the path to the reconciled JSON file.
    """
    logger = get_dagster_logger()
    input_path = Path(json_path)
    output_path = DATA_DIR / "reconciled_table_2.json"

    logger.info(f"Reading JSON from {input_path}")
    data = _read_json(input_path)

    for row in data:
        # Dummy coordinates; in a real implementation, call HERE API.
        row["latitude"] = 0.0
        row["longitude"] = 0.0

    logger.info(f"Writing reconciled data to {output_path}")
    _write_json(data, output_path)

    return str(output_path)


@op(ins={"json_path": In(String)}, out=Out(String))
def calculate_distance_public_transport(json_path: str) -> str:
    """
    Add a dummy distance_to_pt column representing distance to public transport.
    Returns the path to the updated JSON file.
    """
    logger = get_dagster_logger()
    input_path = Path(json_path)
    output_path = DATA_DIR / "distance_pt_2.json"

    logger.info(f"Reading JSON from {input_path}")
    data = _read_json(input_path)

    for row in data:
        # Dummy distance; replace with real spatial calculation.
        row["distance_to_pt"] = 1.0

    logger.info(f"Writing public transport distances to {output_path}")
    _write_json(data, output_path)

    return str(output_path)


@op(ins={"json_path": In(String)}, out=Out(String))
def calculate_distance_residential(json_path: str) -> str:
    """
    Add a dummy distance_to_residential column.
    Returns the path to the final extended JSON file.
    """
    logger = get_dagster_logger()
    input_path = Path(json_path)
    output_path = DATA_DIR / "column_extended_2.json"

    logger.info(f"Reading JSON from {input_path}")
    data = _read_json(input_path)

    for row in data:
        # Dummy distance; replace with real spatial calculation.
        row["distance_to_residential"] = 2.0

    logger.info(f"Writing residential distances to {output_path}")
    _write_json(data, output_path)

    return str(output_path)


@op(ins={"json_path": In(String)}, out=Out(Nothing))
def save_final_data(json_path: str) -> None:
    """
    Export the enriched data to CSV.
    """
    logger = get_dagster_logger()
    input_path = Path(json_path)
    output_path = DATA_DIR / "enriched_data_2.csv"

    logger.info(f"Reading final JSON from {input_path}")
    data = _read_json(input_path)

    logger.info(f"Writing enriched CSV to {output_path}")
    _write_csv(data, output_path)


@job
def medical_facility_accessibility_job():
    """
    Dagster job orchestrating the medical facility accessibility pipeline.
    """
    json_loaded = load_and_modify_data()
    json_reconciled = reconcile_geocode(json_loaded)
    json_pt = calculate_distance_public_transport(json_reconciled)
    json_residential = calculate_distance_residential(json_pt)
    save_final_data(json_residential)


if __name__ == "__main__":
    result = medical_facility_accessibility_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")