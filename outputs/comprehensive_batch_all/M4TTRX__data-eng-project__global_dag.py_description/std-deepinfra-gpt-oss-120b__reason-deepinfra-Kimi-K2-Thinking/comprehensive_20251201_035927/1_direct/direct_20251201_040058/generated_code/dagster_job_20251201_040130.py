from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import (
    In,
    Nothing,
    Out,
    RetryPolicy,
    ResourceDefinition,
    op,
    graph,
    job,
)


# ----------------------------------------------------------------------
# Resource stubs
# ----------------------------------------------------------------------
def postgres_resource(_init_context) -> Dict[str, str]:
    """Stub for a PostgreSQL connection resource."""
    return {"connection_string": "postgresql://user:password@localhost:5432/mydb"}


def redis_resource(_init_context) -> Dict[str, str]:
    """Stub for a Redis connection resource."""
    return {"host": "localhost", "port": 6379}


POSTGRES_RESOURCE = ResourceDefinition.resource_fn(postgres_resource)
REDIS_RESOURCE = ResourceDefinition.resource_fn(redis_resource)


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def _download_csv(url: str) -> pd.DataFrame:
    """Download a CSV from a URL into a DataFrame."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(pd.compat.StringIO(response.text))


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------
@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch nuclear power plant metadata JSON.",
)
def fetch_nuclear_metadata() -> Dict[str, Any]:
    # Placeholder URL for nuclear metadata
    url = "https://example.com/api/nuclear_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return {"metadata_url": url, "data": response.json()}


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch thermal power plant metadata JSON.",
)
def fetch_thermal_metadata() -> Dict[str, Any]:
    url = "https://example.com/api/thermal_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return {"metadata_url": url, "data": response.json()}


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch death records resource list JSON.",
)
def fetch_death_resource_list() -> Dict[str, Any]:
    url = "https://example.com/api/death_records_resources.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return {"resource_list_url": url, "resources": response.json()}


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch city geographic coordinates CSV metadata.",
)
def fetch_city_geo_csv() -> Dict[str, Any]:
    url = "https://example.com/api/cities_geo.csv"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return {"csv_url": url, "content": response.text}


@op(
    out=Out(pd.DataFrame),
    description="Download nuclear power plant CSV data.",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
)
def download_nuclear_csv(metadata: dict) -> pd.DataFrame:
    csv_url = metadata.get("data", {}).get("csv_url", "https://example.com/nuclear.csv")
    return _download_csv(csv_url)


@op(
    out=Out(pd.DataFrame),
    description="Download thermal power plant CSV data.",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
)
def download_thermal_csv(metadata: dict) -> pd.DataFrame:
    csv_url = metadata.get("data", {}).get("csv_url", "https://example.com/thermal.csv")
    return _download_csv(csv_url)


@op(
    out=Out(List[pd.DataFrame]),
    description="Download death record files (multiple CSVs).",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
)
def download_death_files(resource_list: dict) -> List[pd.DataFrame]:
    resources = resource_list.get("resources", [])
    data_frames: List[pd.DataFrame] = []
    for res in resources:
        url = res.get("url")
        if url:
            df = _download_csv(url)
            data_frames.append(df)
    return data_frames


@op(
    out=Out(Nothing),
    description="Create deaths table in PostgreSQL.",
)
def create_deaths_table(_postgres: dict) -> Nothing:
    # Placeholder implementation
    print("Creating deaths table in PostgreSQL.")
    return Nothing


@op(
    out=Out(Nothing),
    description="Create power_plants table in PostgreSQL.",
)
def create_power_plants_table(_postgres: dict) -> Nothing:
    print("Creating power_plants table in PostgreSQL.")
    return Nothing


@op(
    out=Out(pd.DataFrame),
    description="Load death records into Redis (simulated).",
)
def load_death_records_to_redis(
    death_frames: List[pd.DataFrame], _redis: dict
) -> pd.DataFrame:
    # Simulate loading by concatenating frames
    if death_frames:
        combined = pd.concat(death_frames, ignore_index=True)
    else:
        combined = pd.DataFrame()
    print(f"Loaded {len(combined)} death records into Redis (simulated).")
    return combined


@op(
    out=Out(pd.DataFrame),
    description="Clean thermal plant data (rename columns, filter).",
)
def clean_thermal_data(thermal_df: pd.DataFrame) -> pd.DataFrame:
    df = thermal_df.copy()
    rename_map = {"old_name": "new_name"}  # Example mapping
    df.rename(columns=rename_map, inplace=True)
    # Example filter: keep rows where capacity > 0
    df = df[df.get("capacity", 0) > 0]
    print("Cleaned thermal plant data.")
    return df


@op(
    out=Out(pd.DataFrame),
    description="Clean nuclear plant data (rename columns, filter).",
)
def clean_nuclear_data(nuclear_df: pd.DataFrame) -> pd.DataFrame:
    df = nuclear_df.copy()
    rename_map = {"old_nuclear_name": "new_nuclear_name"}
    df.rename(columns=rename_map, inplace=True)
    df = df[df.get("capacity", 0) > 0]
    print("Cleaned nuclear plant data.")
    return df


@op(
    out=Out(bool),
    description="Check if death data is available after loading.",
)
def check_death_data(death_df: pd.DataFrame) -> bool:
    has_data = not death_df.empty
    print(f"Death data availability: {has_data}")
    return has_data


@op(
    out=Out(pd.DataFrame),
    description="Cleanse death records (date formatting, geographic enrichment).",
)
def cleanse_death_records(death_df: pd.DataFrame) -> pd.DataFrame:
    df = death_df.copy()
    if "date_of_death" in df.columns:
        df["date_of_death"] = pd.to_datetime(df["date_of_death"], errors="coerce")
    # Placeholder for geographic enrichment
    df["city_geo_enriched"] = True
    print("Cleansed death records.")
    return df


@op(
    out=Out(Nothing),
    description="Store cleansed death records in PostgreSQL.",
)
def store_death_records_pg(_postgres: dict, cleaned_death_df: pd.DataFrame) -> Nothing:
    print(f"Storing {len(cleaned_death_df)} death records into PostgreSQL.")
    return Nothing


@op(
    out=Out(Nothing),
    description="Clean temporary death data files (simulated).",
)
def clean_temp_death_files() -> Nothing:
    print("Temporary death data files cleaned.")
    return Nothing


@op(
    out=Out(str),
    description="Generate SQL queries for power plant data persistence.",
)
def generate_power_plant_sql(
    thermal_df: pd.DataFrame, nuclear_df: pd.DataFrame
) -> str:
    # Placeholder: generate a simple INSERT statement
    sql_statements = []
    for _, row in pd.concat([thermal_df, nuclear_df], ignore_index=True).iterrows():
        cols = ", ".join(row.index)
        vals = ", ".join([repr(v) for v in row.values])
        sql = f"INSERT INTO power_plants ({cols}) VALUES ({vals});"
        sql_statements.append(sql)
    combined_sql = "\n".join(sql_statements)
    print("Generated SQL for power plant data.")
    return combined_sql


@op(
    out=Out(Nothing),
    description="Store power plant records in PostgreSQL.",
)
def store_power_plant_records_pg(_postgres: dict, sql: str) -> Nothing:
    print("Executing SQL to store power plant records (simulated).")
    # In real implementation, execute the SQL against PostgreSQL.
    return Nothing


@op(
    out=Out(Nothing),
    description="Mark staging phase as complete.",
)
def complete_staging() -> Nothing:
    print("Staging phase completed.")
    return Nothing


# ----------------------------------------------------------------------
# Graph definition
# ----------------------------------------------------------------------
@graph
def etl_graph():
    # Parallel ingestion
    nuclear_meta = fetch_nuclear_metadata()
    thermal_meta = fetch_thermal_metadata()
    death_resource = fetch_death_resource_list()
    city_geo = fetch_city_geo_csv()

    # Process fetched metadata
    nuclear_df = download_nuclear_csv(nuclear_meta)
    thermal_df = download_thermal_csv(thermal_meta)
    death_frames = download_death_files(death_resource)

    # Table creation (depends on downloads)
    create_deaths_table()
    create_power_plants_table()

    # Parallel processing
    death_redis_df = load_death_records_to_redis(death_frames)
    cleaned_thermal = clean_thermal_data(thermal_df)
    cleaned_nuclear = clean_nuclear_data(nuclear_df)

    # Conditional branch based on death data availability
    has_death_data = check_death_data(death_redis_df)

    with if_(has_death_data):
        cleansed_death = cleanse_death_records(death_redis_df)
        store_death_records_pg(cleansed_death)
        clean_temp_death_files()
    # Power plant processing (always runs)
    plant_sql = generate_power_plant_sql(cleaned_thermal, cleaned_nuclear)
    store_power_plant_records_pg(plant_sql)

    # Fan‑in synchronization before completing staging
    complete_staging()


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------
etl_job = etl_graph.to_job(
    name="etl_job",
    resource_defs={"postgres": POSTGRES_RESOURCE, "redis": REDIS_RESOURCE},
    description="ETL pipeline for French government death records and power plant data.",
)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    result = etl_job.execute_in_process()
    if result.success:
        print("ETL job completed successfully.")
    else:
        print("ETL job failed.")