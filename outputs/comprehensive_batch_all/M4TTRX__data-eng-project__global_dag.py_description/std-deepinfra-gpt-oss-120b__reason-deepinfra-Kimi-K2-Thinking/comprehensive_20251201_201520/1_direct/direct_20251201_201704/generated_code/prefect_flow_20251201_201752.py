import io
import os
import logging
from typing import List, Dict, Any

import pandas as pd
import requests
import redis
import sqlalchemy
from prefect import flow, task, get_run_logger

# Configure external connections (replace with real credentials or environment variables)
POSTGRES_CONN = os.getenv(
    "POSTGRES_CONN", "postgresql://user:password@localhost:5432/mydb"
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

engine = sqlalchemy.create_engine(POSTGRES_CONN)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_metadata() -> Dict[str, Any]:
    """Fetch nuclear power plant metadata JSON."""
    url = "https://example.com/api/nuclear/metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data  # Expected to contain a key with the CSV download URL


@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_metadata() -> Dict[str, Any]:
    """Fetch thermal power plant metadata JSON."""
    url = "https://example.com/api/thermal/metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


@task(retries=3, retry_delay_seconds=10)
def fetch_death_resource_list() -> List[str]:
    """Fetch list of URLs for death record files."""
    url = "https://example.com/api/deaths/resources.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()  # Expected to be a list of CSV URLs


@task(retries=3, retry_delay_seconds=10)
def fetch_city_coordinates() -> str:
    """Fetch URL for city geographic coordinates CSV."""
    url = "https://example.com/api/cities/coordinates.csv"
    # Direct URL is returned; could also be fetched from a JSON endpoint
    return url


@task(retries=3, retry_delay_seconds=10)
def download_nuclear_csv(metadata: Dict[str, Any]) -> pd.DataFrame:
    """Download nuclear plant CSV using metadata."""
    csv_url = metadata.get("csv_url")
    if not csv_url:
        raise ValueError("CSV URL not found in nuclear metadata")
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))


@task(retries=3, retry_delay_seconds=10)
def download_thermal_csv(metadata: Dict[str, Any]) -> pd.DataFrame:
    """Download thermal plant CSV using metadata."""
    csv_url = metadata.get("csv_url")
    if not csv_url:
        raise ValueError("CSV URL not found in thermal metadata")
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))


@task(retries=3, retry_delay_seconds=10)
def download_death_files(urls: List[str]) -> List[pd.DataFrame]:
    """Download each death record CSV and return a list of DataFrames."""
    dfs = []
    for url in urls:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dfs.append(pd.read_csv(io.BytesIO(resp.content)))
    return dfs


@task(retries=3, retry_delay_seconds=10)
def load_city_coordinates(csv_url: str) -> pd.DataFrame:
    """Load city coordinates CSV into a DataFrame."""
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))


@task
def create_deaths_table() -> None:
    """Create the deaths table in PostgreSQL if it does not exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR(10),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        other_details JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(create_sql))


@task
def create_power_plants_table() -> None:
    """Create the power_plants table in PostgreSQL if it does not exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_name VARCHAR(255),
        plant_type VARCHAR(50),
        capacity_mw DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        other_details JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(create_sql))


@task
def load_death_records_to_redis(death_dfs: List[pd.DataFrame]) -> None:
    """Load raw death records into Redis for intermediate storage."""
    for df in death_dfs:
        for _, row in df.iterrows():
            redis_client.rpush("death_records_raw", row.to_json())


@task
def clean_thermal_plant_data(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and filter thermal plant data."""
    rename_map = {
        "PlantName": "plant_name",
        "CapacityMW": "capacity_mw",
        "Lat": "latitude",
        "Lon": "longitude",
    }
    df_clean = df.rename(columns=rename_map)
    df_clean = df_clean.dropna(subset=["plant_name", "capacity_mw"])
    df_clean["plant_type"] = "thermal"
    return df_clean


@task
def clean_nuclear_plant_data(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and filter nuclear plant data."""
    rename_map = {
        "Name": "plant_name",
        "Power_MW": "capacity_mw",
        "Latitude": "latitude",
        "Longitude": "longitude",
    }
    df_clean = df.rename(columns=rename_map)
    df_clean = df_clean.dropna(subset=["plant_name", "capacity_mw"])
    df_clean["plant_type"] = "nuclear"
    return df_clean


@task
def check_death_data_exists(death_dfs: List[pd.DataFrame]) -> bool:
    """Return True if any death data files were downloaded."""
    return any(not df.empty for df in death_dfs)


@task
def cleanse_death_records(
    death_dfs: List[pd.DataFrame], city_coords: pd.DataFrame
) -> pd.DataFrame:
    """Standardize dates and enrich with geographic coordinates."""
    # Concatenate all death DataFrames
    combined = pd.concat(death_dfs, ignore_index=True)

    # Example date formatting
    if "date_of_death" in combined.columns:
        combined["death_date"] = pd.to_datetime(
            combined["date_of_death"], errors="coerce"
        ).dt.date

    # Enrich with city coordinates (assuming city_code column exists)
    if "city_code" in combined.columns and not city_coords.empty:
        enriched = combined.merge(
            city_coords[["city_code", "latitude", "longitude"]],
            on="city_code",
            how="left",
            suffixes=("", "_city"),
        )
        combined["latitude"] = enriched["latitude"]
        combined["longitude"] = enriched["longitude"]

    # Keep only needed columns
    needed = ["death_date", "city_code", "latitude", "longitude"]
    return combined[needed]


@task
def store_death_records_postgres(df: pd.DataFrame) -> None:
    """Insert cleansed death records into PostgreSQL."""
    df.to_sql(
        "deaths",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )


@task
def clean_temp_death_files() -> None:
    """Placeholder for cleaning temporary death data files."""
    logger = get_run_logger()
    logger.info("Temporary death data files cleaned up (placeholder).")


@task
def generate_power_plant_sql(
    nuclear_df: pd.DataFrame, thermal_df: pd.DataFrame
) -> List[pd.DataFrame]:
    """Combine cleaned plant data for persistence."""
    combined = pd.concat([nuclear_df, thermal_df], ignore_index=True)
    return [combined]  # Returning list to keep signature similar to downstream task


@task
def store_power_plant_records_postgres(dfs: List[pd.DataFrame]) -> None:
    """Insert power plant records into PostgreSQL."""
    for df in dfs:
        df.to_sql(
            "power_plants",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )


@flow
def etl_pipeline() -> None:
    """Orchestrates the ETL pipeline for French death records and power plant data."""
    logger = get_run_logger()
    logger.info("Starting ETL pipeline")

    # Parallel ingestion
    nuclear_meta_fut = fetch_nuclear_metadata.submit()
    thermal_meta_fut = fetch_thermal_metadata.submit()
    death_res_fut = fetch_death_resource_list.submit()
    city_coords_url_fut = fetch_city_coordinates.submit()

    nuclear_meta = nuclear_meta_fut.result()
    thermal_meta = thermal_meta_fut.result()
    death_resources = death_res_fut.result()
    city_coords_url = city_coords_url_fut.result()

    # Sequential downloads after metadata retrieval
    nuclear_df = download_nuclear_csv(nuclear_meta)
    thermal_df = download_thermal_csv(thermal_meta)
    death_dfs = download_death_files(death_resources)
    city_coords_df = load_city_coordinates(city_coords_url)

    # Table creation
    create_deaths_table()
    create_power_plants_table()

    # Parallel processing of downloaded data
    load_death_fut = load_death_records_to_redis.submit(death_dfs)
    clean_thermal_fut = clean_thermal_plant_data.submit(thermal_df)
    clean_nuclear_fut = clean_nuclear_plant_data.submit(nuclear_df)

    # Ensure parallel tasks complete
    load_death_fut.result()
    clean_thermal_df = clean_thermal_fut.result()
    clean_nuclear_df = clean_nuclear_fut.result()

    # Conditional branch for death data
    has_death_data = check_death_data_exists(death_dfs)
    if has_death_data:
        cleaned_death_df = cleanse_death_records(death_dfs, city_coords_df)
        store_death_records_postgres(cleaned_death_df)
        clean_temp_death_files()
    else:
        logger.info("No death data available; skipping death processing steps.")

    # Power plant persistence
    plant_dfs = generate_power_plant_sql(clean_nuclear_df, clean_thermal_df)
    store_power_plant_records_postgres(plant_dfs)

    logger.info("Staging phase complete")


if __name__ == "__main__":
    etl_pipeline()