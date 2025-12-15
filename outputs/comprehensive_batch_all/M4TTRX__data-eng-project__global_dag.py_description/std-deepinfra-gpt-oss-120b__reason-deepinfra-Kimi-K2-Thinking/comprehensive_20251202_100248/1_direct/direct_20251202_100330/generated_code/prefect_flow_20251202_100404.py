import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from prefect import flow, task
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database and Redis connection placeholders
POSTGRES_CONNECTION_STRING = "postgresql://user:password@localhost:5432/mydb"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

engine = create_engine(POSTGRES_CONNECTION_STRING)


@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_metadata() -> Dict[str, Any]:
    """Fetch nuclear power plant metadata JSON."""
    url = "https://example.com/api/nuclear_metadata.json"
    logger.info("Fetching nuclear metadata from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_metadata() -> Dict[str, Any]:
    """Fetch thermal power plant metadata JSON."""
    url = "https://example.com/api/thermal_metadata.json"
    logger.info("Fetching thermal metadata from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


@task(retries=3, retry_delay_seconds=10)
def fetch_death_resource_list() -> List[Dict[str, Any]]:
    """Fetch list of death record resources."""
    url = "https://example.com/api/death_records_resources.json"
    logger.info("Fetching death records resource list from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json().get("resources", [])


@task(retries=3, retry_delay_seconds=10)
def fetch_city_coords_csv() -> str:
    """Fetch city geographic coordinates CSV."""
    url = "https://example.com/api/city_coords.csv"
    logger.info("Fetching city coordinates CSV from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = Path("data/city_coords.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return str(path)


@task(retries=3, retry_delay_seconds=10)
def download_nuclear_csv(metadata: Dict[str, Any]) -> str:
    """Download nuclear plant CSV using metadata."""
    csv_url = metadata.get("csv_url")
    logger.info("Downloading nuclear CSV from %s", csv_url)
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    path = Path("data/nuclear_plants.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return str(path)


@task(retries=3, retry_delay_seconds=10)
def download_thermal_csv(metadata: Dict[str, Any]) -> str:
    """Download thermal plant CSV using metadata."""
    csv_url = metadata.get("csv_url")
    logger.info("Downloading thermal CSV from %s", csv_url)
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    path = Path("data/thermal_plants.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return str(path)


@task(retries=3, retry_delay_seconds=10)
def download_death_files(resources: List[Dict[str, Any]]) -> List[str]:
    """Download death record files listed in resources."""
    downloaded_paths = []
    for resource in resources:
        file_url = resource.get("url")
        logger.info("Downloading death record file from %s", file_url)
        response = requests.get(file_url, timeout=30)
        response.raise_for_status()
        filename = Path(file_url).name
        path = Path("data/deaths") / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)
        downloaded_paths.append(str(path))
    return downloaded_paths


@task
def create_deaths_table() -> None:
    """Create the deaths table in PostgreSQL."""
    logger.info("Creating deaths table if not exists")
    create_sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR,
        other_fields JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


@task
def create_power_plants_table() -> None:
    """Create the power_plants table in PostgreSQL."""
    logger.info("Creating power_plants table if not exists")
    create_sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_type VARCHAR,
        name VARCHAR,
        capacity_mw NUMERIC,
        city_code VARCHAR,
        other_fields JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


@task(retries=3, retry_delay_seconds=10)
def load_death_records_to_redis(death_file_paths: List[str]) -> bool:
    """Load death records into Redis and return whether data exists."""
    logger.info("Loading death records into Redis")
    # Placeholder: simulate loading and return existence flag
    data_exists = len(death_file_paths) > 0
    time.sleep(1)  # Simulate I/O
    return data_exists


@task
def clean_thermal_plant_data(csv_path: str) -> pd.DataFrame:
    """Clean thermal plant data."""
    logger.info("Cleaning thermal plant data from %s", csv_path)
    df = pd.read_csv(csv_path)
    df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
    df = df[df["capacity_mw"] > 0]  # Example filter
    return df


@task
def clean_nuclear_plant_data(csv_path: str) -> pd.DataFrame:
    """Clean nuclear plant data."""
    logger.info("Cleaning nuclear plant data from %s", csv_path)
    df = pd.read_csv(csv_path)
    df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
    df = df[df["capacity_mw"] > 0]
    return df


@task
def cleanse_death_records() -> pd.DataFrame:
    """Cleanse death records (date formatting, geographic enrichment)."""
    logger.info("Cleansing death records")
    # Placeholder: generate empty DataFrame
    df = pd.DataFrame(columns=["death_date", "city_code", "other_fields"])
    return df


@task
def store_death_records_pg(df: pd.DataFrame) -> None:
    """Store cleansed death records into PostgreSQL."""
    logger.info("Storing death records into PostgreSQL")
    df.to_sql("deaths", con=engine, if_exists="append", index=False)


@task
def clean_temp_death_files(file_paths: List[str]) -> None:
    """Remove temporary death record files."""
    logger.info("Cleaning up temporary death record files")
    for path_str in file_paths:
        path = Path(path_str)
        if path.is_file():
            path.unlink()
            logger.debug("Deleted %s", path)


@task
def generate_plant_sql_queries(
    thermal_df: pd.DataFrame, nuclear_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    """Generate SQL queries (as dicts) for persisting plant data."""
    logger.info("Generating SQL queries for power plant data")
    records = []
    for _, row in pd.concat([thermal_df, nuclear_df]).iterrows():
        records.append(
            {
                "plant_type": row.get("plant_type", "unknown"),
                "name": row.get("name"),
                "capacity_mw": row.get("capacity_mw"),
                "city_code": row.get("city_code"),
                "other_fields": row.to_json(),
            }
        )
    return records


@task
def store_power_plant_records_pg(records: List[Dict[str, Any]]) -> None:
    """Store power plant records into PostgreSQL."""
    logger.info("Storing power plant records into PostgreSQL")
    df = pd.DataFrame(records)
    df.to_sql("power_plants", con=engine, if_exists="append", index=False)


@task
def complete_staging() -> None:
    """Mark staging phase as complete."""
    logger.info("Staging phase completed")


@flow
def etl_pipeline() -> None:
    """Orchestrates the ETL pipeline for French death and power plant data."""
    # Parallel ingestion
    nuclear_meta_fut = fetch_nuclear_metadata.submit()
    thermal_meta_fut = fetch_thermal_metadata.submit()
    death_res_fut = fetch_death_resource_list.submit()
    city_coords_fut = fetch_city_coords_csv.submit()

    nuclear_meta = nuclear_meta_fut.result()
    thermal_meta = thermal_meta_fut.result()
    death_resources = death_res_fut.result()
    _ = city_coords_fut.result()  # Not used further in this example

    # Sequential download based on fetched metadata
    nuclear_csv_fut = download_nuclear_csv.submit(nuclear_meta)
    thermal_csv_fut = download_thermal_csv.submit(thermal_meta)
    death_files_fut = download_death_files.submit(death_resources)

    nuclear_csv = nuclear_csv_fut.result()
    thermal_csv = thermal_csv_fut.result()
    death_files = death_files_fut.result()

    # Table creation
    create_deaths_table()
    create_power_plants_table()

    # Parallel processing
    load_death_fut = load_death_records_to_redis.submit(death_files)
    clean_thermal_fut = clean_thermal_plant_data.submit(thermal_csv)
    clean_nuclear_fut = clean_nuclear_plant_data.submit(nuclear_csv)

    death_data_exists = load_death_fut.result()
    thermal_clean = clean_thermal_fut.result()
    nuclear_clean = clean_nuclear_fut.result()

    # Conditional branch for death data
    if death_data_exists:
        cleansed_deaths = cleanse_death_records()
        store_death_records_pg(cleansed_deaths)
        clean_temp_death_files(death_files)
    else:
        logger.info("No death data available; skipping death processing branch")

    # Power plant data handling
    plant_queries = generate_plant_sql_queries(thermal_clean, nuclear_clean)
    store_power_plant_records_pg(plant_queries)

    # Finalize staging
    complete_staging()


if __name__ == "__main__":
    # Manual trigger; deployment configuration (e.g., schedule) can be added separately.
    etl_pipeline()