import os
import json
import random
from pathlib import Path

import pandas as pd
import requests
from prefect import flow, task


DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


@task
def load_and_modify() -> Path:
    """
    Load the facilities CSV, perform minimal cleaning, and write JSON output.
    """
    csv_path = DATA_DIR / "facilities.csv"
    json_path = DATA_DIR / "table_data_2.json"

    # Placeholder: create a dummy CSV if it does not exist
    if not csv_path.is_file():
        df_dummy = pd.DataFrame(
            {
                "facility_id": [1, 2, 3],
                "name": ["Clinic A", "Hospital B", "Pharmacy C"],
                "address": [
                    "123 Main St, City",
                    "456 Oak Ave, Town",
                    "789 Pine Rd, Village",
                ],
            }
        )
        df_dummy.to_csv(csv_path, index=False)

    df = pd.read_csv(csv_path)

    # Minimal address standardization (placeholder)
    df["address"] = df["address"].str.strip()

    # Write to JSON
    df.to_json(json_path, orient="records", lines=False, force_ascii=False)

    return json_path


@task
def reconcile_geocode(input_json: Path) -> Path:
    """
    Geocode each facility using a placeholder implementation and write JSON output.
    """
    output_json = DATA_DIR / "reconciled_table_2.json"

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy latitude/longitude generation
        record["latitude"] = round(random.uniform(-90, 90), 6)
        record["longitude"] = round(random.uniform(-180, 180), 6)

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_json


@task
def calculate_distance_pt(input_json: Path) -> Path:
    """
    Calculate distance to the nearest public transport stop (dummy values) and write JSON output.
    """
    output_json = DATA_DIR / "distance_pt_2.json"

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["distance_to_pt"] = round(random.uniform(0, 5000), 2)  # meters

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_json


@task
def calculate_distance_residential(input_json: Path) -> Path:
    """
    Calculate distance to the nearest residential area (dummy values) and write JSON output.
    """
    output_json = DATA_DIR / "column_extended_2.json"

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["distance_to_residential"] = round(random.uniform(0, 5000), 2)  # meters

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_json


@task
def save_final_data(input_json: Path) -> Path:
    """
    Export the enriched data to CSV.
    """
    output_csv = DATA_DIR / "enriched_data_2.csv"

    with input_json.open("r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df.to_csv(output_csv, index=False)

    return output_csv


@flow
def medical_facility_accessibility_flow():
    """
    Orchestrates the medical facility accessibility pipeline.
    """
    # Step 1: Load & modify data
    modified_json = load_and_modify()

    # Step 2: Reconciliation (Geocoding)
    reconciled_json = reconcile_geocode(modified_json)

    # Step 3: Distance calculation to public transport
    distance_pt_json = calculate_distance_pt(reconciled_json)

    # Step 4: Distance calculation to residential areas
    final_json = calculate_distance_residential(distance_pt_json)

    # Step 5: Save final data to CSV
    save_final_data(final_json)


# Deployment/schedule configuration can be added via Prefect UI or programmatic deployment.
# Example: schedule = CronSchedule("0 6 * * *")  # daily at 06:00 UTC


if __name__ == "__main__":
    medical_facility_accessibility_flow()