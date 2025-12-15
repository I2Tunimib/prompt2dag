from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List

import prefect
from prefect import flow, task

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_metadata() -> Dict[str, Any]:
    """Fetch nuclear power plant metadata JSON."""
    logger.info("Fetching nuclear power plant metadata...")
    # Simulated response
    metadata = {"url": "https://example.com/nuclear_plants.csv"}
    time.sleep(1)
    logger.debug("Nuclear metadata: %s", metadata)
    return metadata


@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_metadata() -> Dict[str, Any]:
    """Fetch thermal power plant metadata JSON."""
    logger.info("Fetching thermal power plant metadata...")
    metadata = {"url": "https://example.com/thermal_plants.csv"}
    time.sleep(1)
    logger.debug("Thermal metadata: %s", metadata)
    return metadata


@task(retries=3, retry_delay_seconds=10)
def fetch_death_resource_list() -> List[Dict[str, Any]]:
    """Fetch death records resource list."""
    logger.info("Fetching death records resource list...")
    resources = [
        {"url": "https://example.com/deaths_2023_part1.csv"},
        {"url": "https://example.com/deaths_2023_part2.csv"},
    ]
    time.sleep(1)
    logger.debug("Death resources: %s", resources)
    return resources


@task(retries=3, retry_delay_seconds=10)
def fetch_city_geo_csv() -> Dict[str, Any]:
    """Fetch city geographic coordinates CSV metadata."""
    logger.info("Fetching city geographic coordinates CSV metadata...")
    metadata = {"url": "https://example.com/cities_geo.csv"}
    time.sleep(1)
    logger.debug("City geo metadata: %s", metadata)
    return metadata


@task
def download_nuclear_csv(metadata: Dict[str, Any]) -> Path:
    """Download nuclear power plant CSV data."""
    logger.info("Downloading nuclear CSV from %s", metadata["url"])
    file_path = Path("/tmp/nuclear_plants.csv")
    file_path.write_text("id,name,capacity\n1,Nuclear A,1000\n")
    logger.debug("Nuclear CSV saved to %s", file_path)
    return file_path


@task
def download_thermal_csv(metadata: Dict[str, Any]) -> Path:
    """Download thermal power plant CSV data."""
    logger.info("Downloading thermal CSV from %s", metadata["url"])
    file_path = Path("/tmp/thermal_plants.csv")
    file_path.write_text("id,name,capacity\n10,Thermal X,500\n")
    logger.debug("Thermal CSV saved to %s", file_path)
    return file_path


@task
def download_death_records(resources: List[Dict[str, Any]]) -> List[Path]:
    """Download death record files."""
    logger.info("Downloading death record files...")
    downloaded = []
    for idx, res in enumerate(resources, start=1):
        file_path = Path(f"/tmp/deaths_part{idx}.csv")
        file_path.write_text("id,date,city\n100,2023-01-01,Paris\n")
        downloaded.append(file_path)
        logger.debug("Downloaded %s", file_path)
    return downloaded


@task
def create_deaths_table() -> None:
    """Create the deaths table in PostgreSQL."""
    logger.info("Creating deaths table in PostgreSQL...")
    # Placeholder for actual DDL execution
    time.sleep(0.5)
    logger.debug("Deaths table created.")


@task
def create_power_plants_table() -> None:
    """Create the power_plants table in PostgreSQL."""
    logger.info("Creating power_plants table in PostgreSQL...")
    time.sleep(0.5)
    logger.debug("Power plants table created.")


@task
def load_death_records_to_redis(death_files: List[Path]) -> str:
    """Load death records into Redis (simulated)."""
    logger.info("Loading death records into Redis...")
    # Simulated Redis key
    redis_key = "death_records"
    logger.debug("Stored %d files under Redis key %s", len(death_files), redis_key)
    return redis_key


@task
def clean_thermal_data(csv_path: Path) -> List[Dict[str, Any]]:
    """Clean thermal plant data."""
    logger.info("Cleaning thermal plant data from %s", csv_path)
    # Simulated cleaning
    cleaned = [{"id": 10, "name": "Thermal X", "capacity": 500}]
    logger.debug("Cleaned thermal data: %s", cleaned)
    return cleaned


@task
def clean_nuclear_data(csv_path: Path) -> List[Dict[str, Any]]:
    """Clean nuclear plant data."""
    logger.info("Cleaning nuclear plant data from %s", csv_path)
    cleaned = [{"id": 1, "name": "Nuclear A", "capacity": 1000}]
    logger.debug("Cleaned nuclear data: %s", cleaned)
    return cleaned


@task
def check_death_data_availability(death_files: List[Path]) -> bool:
    """Check if death data files are present."""
    has_data = len(death_files) > 0
    logger.info("Death data availability: %s", has_data)
    return has_data


@task
def cleanse_death_records(death_files: List[Path]) -> List[Dict[str, Any]]:
    """Cleanse death records (date formatting, geographic enrichment)."""
    logger.info("Cleansing death records...")
    cleansed = []
    for file in death_files:
        # Simulated read and cleanse
        cleansed.append({"id": 100, "date": "2023-01-01", "city": "Paris", "insee": "75056"})
    logger.debug("Cleansed death records: %s", cleansed)
    return cleansed


@task
def store_death_records_to_postgres(cleaned_death: List[Dict[str, Any]]) -> None:
    """Store cleansed death records into PostgreSQL."""
    logger.info("Storing %d death records into PostgreSQL...", len(cleaned_death))
    time.sleep(0.5)
    logger.debug("Death records stored.")


@task
def clean_temp_death_files(death_files: List[Path]) -> None:
    """Remove temporary death data files."""
    logger.info("Cleaning up temporary death data files...")
    for file in death_files:
        try:
            file.unlink()
            logger.debug("Deleted %s", file)
        except FileNotFoundError:
            logger.warning("File not found during cleanup: %s", file)


@task
def generate_power_plant_sql(
    thermal_data: List[Dict[str, Any]], nuclear_data: List[Dict[str, Any]]
) -> List[str]:
    """Generate SQL statements for power plant data persistence."""
    logger.info("Generating SQL statements for power plant data...")
    statements = []
    for row in thermal_data + nuclear_data:
        stmt = (
            f"INSERT INTO power_plants (id, name, capacity) VALUES "
            f"({row['id']}, '{row['name']}', {row['capacity']}) "
            f"ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, capacity = EXCLUDED.capacity;"
        )
        statements.append(stmt)
    logger.debug("Generated SQL statements: %s", statements)
    return statements


@task
def store_power_plant_records_to_postgres(sql_statements: List[str]) -> None:
    """Execute generated SQL statements to store power plant records."""
    logger.info("Storing power plant records into PostgreSQL...")
    for stmt in sql_statements:
        logger.debug("Executing SQL: %s", stmt)
        # Placeholder for actual execution
        time.sleep(0.1)
    logger.debug("Power plant records stored.")


@task
def complete_staging() -> None:
    """Mark the staging phase as complete."""
    logger.info("Staging phase completed.")


@flow
def etl_pipeline() -> None:
    """Orchestrate the ETL pipeline for French death records and power plant data."""
    # Parallel ingestion
    ingestion_futures = {
        "nuclear_meta": fetch_nuclear_metadata.submit(),
        "thermal_meta": fetch_thermal_metadata.submit(),
        "death_resources": fetch_death_resource_list.submit(),
        "city_geo": fetch_city_geo_csv.submit(),
    }

    nuclear_meta = ingestion_futures["nuclear_meta"].result()
    thermal_meta = ingestion_futures["thermal_meta"].result()
    death_resources = ingestion_futures["death_resources"].result()
    _ = ingestion_futures["city_geo"].result()  # City geo data not used further in this example

    # Process fetched metadata
    nuclear_csv = download_nuclear_csv(nuclear_meta)
    thermal_csv = download_thermal_csv(thermal_meta)
    death_files = download_death_records(death_resources)

    # Create PostgreSQL tables
    create_deaths_table()
    create_power_plants_table()

    # Parallel data processing
    processing_futures = {
        "load_death_redis": load_death_records_to_redis.submit(death_files),
        "clean_thermal": clean_thermal_data.submit(thermal_csv),
        "clean_nuclear": clean_nuclear_data.submit(nuclear_csv),
    }

    _ = processing_futures["load_death_redis"].result()
    thermal_clean = processing_futures["clean_thermal"].result()
    nuclear_clean = processing_futures["clean_nuclear"].result()

    # Conditional branching based on death data availability
    if check_death_data_availability(death_files):
        cleaned_death = cleanse_death_records(death_files)
        store_death_records_to_postgres(cleaned_death)
        clean_temp_death_files(death_files)
    else:
        logger.info("No death data available; skipping death data processing.")

    # Power plant data processing
    sql_statements = generate_power_plant_sql(thermal_clean, nuclear_clean)
    store_power_plant_records_to_postgres(sql_statements)

    # Complete staging
    complete_staging()


if __name__ == "__main__":
    etl_pipeline()