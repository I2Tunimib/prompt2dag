import os
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import requests
from prefect import flow, task


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


@task
def load_and_modify(csv_filename: str) -> Path:
    """Load stations CSV, parse dates, standardize locations, and write JSON."""
    input_path = DATA_DIR / csv_filename
    output_path = DATA_DIR / "table_data_2.json"

    records: List[Dict[str, Any]] = []
    with input_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Parse installation_date
            date_str = row.get("installation_date", "")
            try:
                parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                row["installation_date"] = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                row["installation_date"] = None

            # Standardize location name (simple title case)
            location = row.get("location", "")
            row["location"] = location.title()

            records.append(row)

    with output_path.open("w", encoding="utf-8") as jsonfile:
        json.dump(records, jsonfile, ensure_ascii=False, indent=2)

    return output_path


@task
def reconcile_geocode(json_path: Path) -> Path:
    """Geocode location field and add latitude/longitude."""
    output_path = DATA_DIR / "reconciled_table_2.json"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        location = record.get("location", "")
        # Dummy geocoding: use hash to generate lat/lon
        hash_val = abs(hash(location)) % 1000
        record["latitude"] = 40.0 + (hash_val % 100) * 0.01
        record["longitude"] = -100.0 + (hash_val % 100) * 0.01

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def open_meteo_extension(json_path: Path) -> Path:
    """Add historical weather data based on geocoded location and installation date."""
    output_path = DATA_DIR / "open_meteo_2.json"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy weather data
        record["apparent_temperature_max"] = 30.0
        record["apparent_temperature_min"] = 15.0
        record["precipitation_sum"] = 5.0
        record["precipitation_hours"] = 2.0

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def land_use_extension(json_path: Path) -> Path:
    """Add land use classification based on location."""
    output_path = DATA_DIR / "land_use_2.json"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy land use type
        record["land_use_type"] = "urban"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def population_density_extension(json_path: Path) -> Path:
    """Add population density data for surrounding area."""
    output_path = DATA_DIR / "pop_density_2.json"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy population density
        record["population_density"] = 1500  # persons per km²

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def environmental_calculation(json_path: Path) -> Path:
    """Compute environmental risk score based on combined data."""
    output_path = DATA_DIR / "column_extended_2.json"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        precip = record.get("precipitation_sum", 0)
        pop_density = record.get("population_density", 0)
        # Simple risk formula (placeholder)
        risk_score = (precip * 0.3) + (pop_density * 0.0005)
        record["risk_score"] = round(risk_score, 3)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


@task
def save_final_data(json_path: Path) -> Path:
    """Export the enriched dataset to CSV."""
    output_path = DATA_DIR / "enriched_data_2.csv"

    with json_path.open(encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        raise ValueError("No records to save.")

    fieldnames = list(records[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    return output_path


@flow
def environmental_monitoring_pipeline():
    """Orchestrates the environmental monitoring data pipeline."""
    # Step 1: Load & Modify Data
    modified_json = load_and_modify("stations.csv")

    # Step 2: Reconciliation (Geocoding)
    geocoded_json = reconcile_geocode(modified_json)

    # Step 3: OpenMeteo Data Extension
    weather_json = open_meteo_extension(geocoded_json)

    # Step 4: Land Use Extension
    land_use_json = land_use_extension(weather_json)

    # Step 5: Population Density Extension
    pop_density_json = population_density_extension(land_use_json)

    # Step 6: Environmental Calculation
    calculated_json = environmental_calculation(pop_density_json)

    # Step 7: Save Final Data
    final_csv = save_final_data(calculated_json)

    return final_csv


if __name__ == "__main__":
    # Local execution
    result_path = environmental_monitoring_pipeline()
    print(f"Pipeline completed. Final CSV saved at: {result_path}")