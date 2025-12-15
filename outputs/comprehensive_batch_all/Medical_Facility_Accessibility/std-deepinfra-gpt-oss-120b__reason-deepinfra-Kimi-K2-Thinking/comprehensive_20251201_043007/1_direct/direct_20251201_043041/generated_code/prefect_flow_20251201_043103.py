import os
import json
import logging
from pathlib import Path
from typing import List, Dict

import pandas as pd
from prefect import flow, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variable for shared data directory
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


@task(retries=1, retry_delay_seconds=10)
def load_and_modify() -> Path:
    """
    Load the facilities CSV, clean address column, and write to JSON.
    Returns the path to the generated JSON file.
    """
    input_path = DATA_DIR / "facilities.csv"
    output_path = DATA_DIR / "table_data_2.json"

    logger.info("Loading facilities from %s", input_path)
    df = pd.read_csv(input_path, dtype=str)

    # Basic cleaning: strip whitespace from address column
    if "address" in df.columns:
        df["address"] = df["address"].str.strip()
    else:
        logger.warning("Column 'address' not found in input CSV.")

    # Convert to list of dicts and write JSON
    records: List[Dict] = df.to_dict(orient="records")
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Saved cleaned data to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def reconcile_geocode(input_json: Path) -> Path:
    """
    Simulate geocoding by adding dummy latitude and longitude fields.
    Returns the path to the reconciled JSON file.
    """
    output_path = DATA_DIR / "reconciled_table_2.json"

    logger.info("Reading data for geocoding from %s", input_json)
    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    # Add dummy coordinates (real implementation would call HERE API)
    for record in records:
        record.setdefault("latitude", 0.0)
        record.setdefault("longitude", 0.0)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Geocoding completed, saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def calculate_distance_pt(input_json: Path) -> Path:
    """
    Simulate distance calculation to public transport.
    Adds a 'distance_to_pt' column with dummy values.
    Returns the path to the updated JSON file.
    """
    output_path = DATA_DIR / "distance_pt_2.json"

    logger.info("Calculating distance to public transport from %s", input_json)
    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["distance_to_pt"] = 0.0  # placeholder value

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Public transport distance added, saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def calculate_distance_residential(input_json: Path) -> Path:
    """
    Simulate distance calculation to residential areas.
    Adds a 'distance_to_residential' column with dummy values.
    Returns the path to the final JSON file.
    """
    output_path = DATA_DIR / "column_extended_2.json"

    logger.info("Calculating distance to residential areas from %s", input_json)
    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["distance_to_residential"] = 0.0  # placeholder value

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Residential distance added, saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def save_final_data(input_json: Path) -> Path:
    """
    Export the enriched JSON data to a CSV file.
    Returns the path to the CSV file.
    """
    output_path = DATA_DIR / "enriched_data_2.csv"

    logger.info("Exporting final data from %s to CSV", input_json)
    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)

    logger.info("Final CSV saved to %s", output_path)
    return output_path


@flow
def medical_facility_accessibility_flow():
    """
    Orchestrates the medical facility accessibility pipeline.
    """
    # Step 1: Load & modify data
    json_1 = load_and_modify()

    # Step 2: Reconciliation (Geocoding)
    json_2 = reconcile_geocode(json_1)

    # Step 3: Distance calculation to public transport
    json_3 = calculate_distance_pt(json_2)

    # Step 4: Distance calculation to residential areas
    json_4 = calculate_distance_residential(json_3)

    # Step 5: Save final data to CSV
    save_final_data(json_4)


if __name__ == "__main__":
    # For local execution; in production, configure a Prefect deployment with a schedule if needed.
    medical_facility_accessibility_flow()