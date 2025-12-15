import json
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict

import prefect
from prefect import flow, task
from prefect.logging import get_run_logger

# Environment variable for shared data directory
DATA_DIR = Path.getenv("DATA_DIR", default="data")
DATA_DIR = Path(DATA_DIR)


@task(retries=1, retry_delay_seconds=10)
def load_and_modify() -> Path:
    """Load stations CSV, parse dates, standardize locations, and save as JSON."""
    logger = get_run_logger()
    input_path = DATA_DIR / "stations.csv"
    output_path = DATA_DIR / "table_data_2.json"

    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    records: List[Dict] = []
    with input_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Parse installation_date
            date_str = row.get("installation_date", "")
            try:
                parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                row["installation_date"] = parsed_date.isoformat()
            except ValueError:
                logger.warning(f"Unable to parse date '{date_str}' for row {row}")

            # Standardize location name (simple title case)
            location = row.get("location", "")
            row["location"] = location.title()

            records.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as jsonfile:
        json.dump(records, jsonfile, ensure_ascii=False, indent=2)

    logger.info("Loaded and modified data saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def reconcile_geocode(input_path: Path) -> Path:
    """Geocode locations and add latitude/longitude to the dataset."""
    logger = get_run_logger()
    output_path = DATA_DIR / "reconciled_table_2.json"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy geocoding: generate pseudo coordinates based on hash
        location = record.get("location", "")
        hash_val = abs(hash(location))
        record["latitude"] = (hash_val % 180) - 90  # -90 to +90
        record["longitude"] = (hash_val % 360) - 180  # -180 to +180

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Geocoding completed, output saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def open_meteo_extension(input_path: Path) -> Path:
    """Add historical weather data based on location and installation date."""
    logger = get_run_logger()
    output_path = DATA_DIR / "open_meteo_2.json"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy weather values
        record["apparent_temperature_max"] = 30.0
        record["apparent_temperature_min"] = 15.0
        record["precipitation_sum"] = 5.0
        record["precipitation_hours"] = 2.0

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("OpenMeteo extension added, output saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def land_use_extension(input_path: Path) -> Path:
    """Add land use classification based on location."""
    logger = get_run_logger()
    output_path = DATA_DIR / "land_use_2.json"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy land use type
        record["land_use_type"] = "urban"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Land use extension added, output saved to %s", output_path)
    return output_path


@task(retries=1, retry_delay_seconds=10)
def population_density_extension(input_path: Path) -> Path:
    """Add population density data for each station location."""
    logger = get_run_logger()
    output_path = DATA_DIR / "pop_density_2.json"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Dummy population density
        record["population_density"] = 1200  # persons per km²

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info(
        "Population density extension added, output saved to %s", output_path
    )
    return output_path


@task(retries=1, retry_delay_seconds=10)
def environmental_calculation(input_path: Path) -> Path:
    """Compute environmental risk score based on combined data."""
    logger = get_run_logger()
    output_path = DATA_DIR / "column_extended_2.json"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    for record in records:
        # Simple risk score formula (dummy)
        precip = record.get("precipitation_sum", 0)
        pop_density = record.get("population_density", 0)
        land_use = record.get("land_use_type", "unknown")
        land_use_factor = 1.0 if land_use == "urban" else 0.5
        risk_score = (precip * 0.3) + (pop_density * 0.0005) * land_use_factor
        record["risk_score"] = round(risk_score, 3)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info(
        "Environmental risk calculation completed, output saved to %s",
        output_path,
    )
    return output_path


@task(retries=1, retry_delay_seconds=10)
def save_final_data(input_path: Path) -> Path:
    """Export the enriched dataset to CSV."""
    logger = get_run_logger()
    output_path = DATA_DIR / "enriched_data_2.csv"

    with input_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        raise ValueError("No records to save.")

    fieldnames = list(records[0].keys())
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    logger.info("Final enriched data saved to %s", output_path)
    return output_path


@flow
def environmental_monitoring_pipeline():
    """Orchestrates the full environmental monitoring data pipeline."""
    logger = get_run_logger()
    logger.info("Starting Environmental Monitoring Network Pipeline")

    # Step 1: Load & Modify Data
    step1_path = load_and_modify()

    # Step 2: Reconciliation (Geocoding)
    step2_path = reconcile_geocode(step1_path)

    # Step 3: OpenMeteo Data Extension
    step3_path = open_meteo_extension(step2_path)

    # Step 4: Land Use Extension
    step4_path = land_use_extension(step3_path)

    # Step 5: Population Density Extension
    step5_path = population_density_extension(step4_path)

    # Step 6: Environmental Calculation (Column Extension)
    step6_path = environmental_calculation(step5_path)

    # Step 7: Save Final Data
    final_path = save_final_data(step6_path)

    logger.info("Pipeline completed successfully. Final file: %s", final_path)


# Optional: Deployment/schedule configuration can be added via Prefect UI or programmatic deployment.
# Example: schedule = CronSchedule("0 2 * * *")  # daily at 02:00 UTC

if __name__ == "__main__":
    environmental_monitoring_pipeline()