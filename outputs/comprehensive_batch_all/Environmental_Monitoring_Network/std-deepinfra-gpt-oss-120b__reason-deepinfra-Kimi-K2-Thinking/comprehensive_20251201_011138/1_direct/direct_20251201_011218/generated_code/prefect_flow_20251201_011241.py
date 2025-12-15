import os
import csv
import json
from pathlib import Path
from typing import List, Dict

from prefect import flow, task


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET_ID = 2


def _ensure_dir(path: Path) -> None:
    """Create parent directories for the given path if they do not exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


@task
def load_and_modify() -> Path:
    """
    Load the stations CSV, parse the installation date, standardize location names,
    and write the result as JSON.

    Returns:
        Path to the generated JSON file.
    """
    input_file = DATA_DIR / "stations.csv"
    output_file = DATA_DIR / "table_data_2.json"

    if not input_file.is_file():
        raise FileNotFoundError(f"Input CSV not found: {input_file}")

    records: List[Dict] = []
    with input_file.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Parse date column (keep as string for simplicity)
            row["installation_date"] = row.get("installation_date", "").strip()
            # Standardize location name
            location = row.get("location", "").strip()
            row["location"] = location.title()
            records.append(row)

    _ensure_dir(output_file)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_file


@task
def reconcile_geocode(input_path: Path) -> Path:
    """
    Geocode station locations by adding dummy latitude and longitude fields.

    Args:
        input_path: Path to the JSON file from the previous step.

    Returns:
        Path to the geocoded JSON file.
    """
    output_path = DATA_DIR / "reconciled_table_2.json"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy coordinates; in a real implementation call a geocoding API.
        record["latitude"] = 0.0
        record["longitude"] = 0.0

    _ensure_dir(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def open_meteo_extension(input_path: Path) -> Path:
    """
    Add historical weather data fields with placeholder values.

    Args:
        input_path: Path to the geocoded JSON file.

    Returns:
        Path to the JSON file enriched with weather data.
    """
    output_path = DATA_DIR / "open_meteo_2.json"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["apparent_temperature_max"] = None
        record["apparent_temperature_min"] = None
        record["precipitation_sum"] = None
        record["precipitation_hours"] = None

    _ensure_dir(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def land_use_extension(input_path: Path) -> Path:
    """
    Add land use classification with a placeholder value.

    Args:
        input_path: Path to the JSON file with weather data.

    Returns:
        Path to the JSON file enriched with land use information.
    """
    output_path = DATA_DIR / "land_use_2.json"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["land_use_type"] = "unknown"

    _ensure_dir(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def population_density_extension(input_path: Path) -> Path:
    """
    Add population density data with a placeholder value.

    Args:
        input_path: Path to the JSON file with land use data.

    Returns:
        Path to the JSON file enriched with population density.
    """
    output_path = DATA_DIR / "pop_density_2.json"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        record["population_density"] = None

    _ensure_dir(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def environmental_calculation(input_path: Path) -> Path:
    """
    Compute a simple environmental risk score based on available fields.

    Args:
        input_path: Path to the JSON file with population density data.

    Returns:
        Path to the JSON file with an added risk_score column.
    """
    output_path = DATA_DIR / "column_extended_2.json"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Simple placeholder calculation: sum of precipitation_sum and population_density
        precip = record.get("precipitation_sum") or 0
        pop_density = record.get("population_density") or 0
        # Convert to numeric if possible
        try:
            precip_val = float(precip)
        except (TypeError, ValueError):
            precip_val = 0.0
        try:
            pop_val = float(pop_density)
        except (TypeError, ValueError):
            pop_val = 0.0
        record["risk_score"] = precip_val + pop_val

    _ensure_dir(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def save_final_data(input_path: Path) -> Path:
    """
    Export the enriched dataset to CSV.

    Args:
        input_path: Path to the final JSON file.

    Returns:
        Path to the generated CSV file.
    """
    output_path = DATA_DIR / "enriched_data_2.csv"

    with input_path.open(encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        raise ValueError("No records to write to CSV.")

    fieldnames = list(records[0].keys())

    _ensure_dir(output_path)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    return output_path


@flow
def environmental_monitoring_pipeline() -> None:
    """
    Orchestrates the environmental monitoring data pipeline.
    """
    json_1 = load_and_modify()
    json_2 = reconcile_geocode(json_1)
    json_3 = open_meteo_extension(json_2)
    json_4 = land_use_extension(json_3)
    json_5 = population_density_extension(json_4)
    json_6 = environmental_calculation(json_5)
    csv_path = save_final_data(json_6)

    # Log final output location
    print(f"Pipeline completed. Enriched CSV saved at: {csv_path}")


# Deployment schedule can be configured separately, e.g., using Prefect
# deployments with a cron schedule.

if __name__ == "__main__":
    environmental_monitoring_pipeline()