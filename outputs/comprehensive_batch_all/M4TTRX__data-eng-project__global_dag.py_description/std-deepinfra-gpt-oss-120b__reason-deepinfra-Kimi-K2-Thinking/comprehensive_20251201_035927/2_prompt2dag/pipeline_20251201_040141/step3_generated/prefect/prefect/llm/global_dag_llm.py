# -*- coding: utf-8 -*-
"""
Generated Prefect 2.x ETL pipeline for processing French government death records
and power plant data.

Generation timestamp: 2024-06-28 12:00:00 UTC
Author: Prefect Code Generator
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import Secret
from prefect.filesystems import LocalFileSystem

# -------------------------------------------------------------------------
# Block loading helpers
# -------------------------------------------------------------------------

def _load_secret(secret_name: str) -> Secret:
    """
    Load a Prefect Secret block by name.

    Args:
        secret_name: Name of the Secret block.

    Returns:
        Secret block instance.

    Raises:
        ValueError: If the secret block cannot be found.
    """
    try:
        return Secret.load(secret_name)
    except Exception as exc:
        raise ValueError(f"Unable to load secret block '{secret_name}': {exc}") from exc


def _load_filesystem(block_name: str) -> LocalFileSystem:
    """
    Load a Prefect LocalFileSystem block by name.

    Args:
        block_name: Name of the LocalFileSystem block.

    Returns:
        LocalFileSystem block instance.

    Raises:
        ValueError: If the filesystem block cannot be found.
    """
    try:
        return LocalFileSystem.load(block_name)
    except Exception as exc:
        raise ValueError(
            f"Unable to load filesystem block '{block_name}': {exc}"
        ) from exc


# -------------------------------------------------------------------------
# Configuration (block names)
# -------------------------------------------------------------------------

CITY_GEO_API_SECRET = "city_geo_api"
NUCLEAR_PLANTS_API_SECRET = "nuclear_plants_api"
THERMAL_PLANTS_API_SECRET = "thermal_plants_api"
DEATH_RECORDS_API_SECRET = "death_records_api"

REDIS_CACHE_SECRET = "redis_cache"
POSTGRES_DB_SECRET = "postgres_db"

FS_INGESTION = "filesystem_ingestion"
FS_STAGING = "filesystem_staging"
FS_SQL_TMP = "filesystem_sql_tmp"


# -------------------------------------------------------------------------
# Utility tasks
# -------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def fetch_json(api_url: str, api_key: str) -> Dict[str, Any]:
    """
    Generic HTTP GET request that returns JSON payload.

    Args:
        api_url: Full URL of the API endpoint.
        api_key: API key or token for authentication.

    Returns:
        Parsed JSON response.

    Raises:
        RuntimeError: If the request fails or returns a non‑200 status.
    """
    logger = get_run_logger()
    headers = {"Authorization": f"Bearer {api_key}"}
    logger.info("Fetching data from %s", api_url)

    response = requests.get(api_url, headers=headers, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch data from {api_url}: {response.status_code} {response.text}"
        )
    return response.json()


@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def write_dataframe_to_parquet(df: pd.DataFrame, path: str) -> None:
    """
    Write a pandas DataFrame to a Parquet file.

    Args:
        df: DataFrame to write.
        path: Destination file path (including filename).
    """
    logger = get_run_logger()
    logger.info("Writing DataFrame to Parquet at %s", path)
    df.to_parquet(path, index=False)


@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def read_parquet_to_dataframe(path: str) -> pd.DataFrame:
    """
    Read a Parquet file into a pandas DataFrame.

    Args:
        path: Path to the Parquet file.

    Returns:
        DataFrame containing the file's data.
    """
    logger = get_run_logger()
    logger.info("Reading Parquet file from %s", path)
    return pd.read_parquet(path)


@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def load_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    if_exists: str = "replace",
) -> None:
    """
    Load a DataFrame into a PostgreSQL table using SQLAlchemy.

    Args:
        df: DataFrame to load.
        table_name: Destination table name.
        engine: SQLAlchemy engine connected to PostgreSQL.
        if_exists: Behavior if the table already exists (default: replace).
    """
    logger = get_run_logger()
    logger.info("Loading DataFrame into PostgreSQL table %s", table_name)
    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)


# -------------------------------------------------------------------------
# Domain‑specific extraction tasks
# -------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def extract_city_geo_data() -> pd.DataFrame:
    """
    Extract city geographic data from the public API.

    Returns:
        DataFrame with city geographic information.
    """
    logger = get_run_logger()
    secret = _load_secret(CITY_GEO_API_SECRET)
    api_key = secret.get()
    api_url = "https://static.data.gouv.fr/api/cities/geo"
    raw_json = fetch_json.run(api_url, api_key)

    # Assume the API returns a list of city objects
    df = pd.json_normalize(raw_json)
    logger.info("Extracted %d city records", len(df))
    return df


@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def extract_nuclear_plants_data() -> pd.DataFrame:
    """
    Extract nuclear power plant data from the public API.

    Returns:
        DataFrame with nuclear plant information.
    """
    logger = get_run_logger()
    secret = _load_secret(NUCLEAR_PLANTS_API_SECRET)
    api_key = secret.get()
    api_url = "https://data.gouv.fr/api/nuclear-plants"
    raw_json = fetch_json.run(api_url, api_key)

    df = pd.json_normalize(raw_json)
    logger.info("Extracted %d nuclear plant records", len(df))
    return df


@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def extract_thermal_plants_data() -> pd.DataFrame:
    """
    Extract thermal power plant data from the public API.

    Returns:
        DataFrame with thermal plant information.
    """
    logger = get_run_logger()
    secret = _load_secret(THERMAL_PLANTS_API_SECRET)
    api_key = secret.get()
    api_url = "https://data.gouv.fr/api/thermal-plants"
    raw_json = fetch_json.run(api_url, api_key)

    df = pd.json_normalize(raw_json)
    logger.info("Extracted %d thermal plant records", len(df))
    return df


@task(retries=3, retry_delay_seconds=60, timeout_seconds=300)
def extract_death_records_data() -> pd.DataFrame:
    """
    Extract death records from the public API.

    Returns:
        DataFrame with death record information.
    """
    logger = get_run_logger()
    secret = _load_secret(DEATH_RECORDS_API_SECRET)
    api_key = secret.get()
    api_url = "https://data.gouv.fr/api/death-records"
    raw_json = fetch_json.run(api_url, api_key)

    df = pd.json_normalize(raw_json)
    logger.info("Extracted %d death record rows", len(df))
    return df


# -------------------------------------------------------------------------
# Transformation tasks
# -------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def clean_city_geo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform basic cleaning on city geographic data.

    Args:
        df: Raw city data.

    Returns:
        Cleaned DataFrame.
    """
    logger = get_run_logger()
    logger.info("Cleaning city geographic data")
    # Example cleaning steps
    df = df.dropna(subset=["city_code", "latitude", "longitude"])
    df["city_code"] = df["city_code"].astype(str).str.zfill(5)
    return df


@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def clean_nuclear_plants(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean nuclear plant dataset.

    Args:
        df: Raw nuclear plant data.

    Returns:
        Cleaned DataFrame.
    """
    logger = get_run_logger()
    logger.info("Cleaning nuclear plant data")
    df = df.dropna(subset=["plant_id", "capacity_mw"])
    df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")
    return df


@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def clean_thermal_plants(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean thermal plant dataset.

    Args:
        df: Raw thermal plant data.

    Returns:
        Cleaned DataFrame.
    """
    logger = get_run_logger()
    logger.info("Cleaning thermal plant data")
    df = df.dropna(subset=["plant_id", "fuel_type"])
    return df


@task(retries=2, retry_delay_seconds=30, timeout_seconds=180)
def clean_death_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean death records dataset.

    Args:
        df: Raw death records.

    Returns:
        Cleaned DataFrame.
    """
    logger = get_run_logger()
    logger.info("Cleaning death records data")
    df = df.dropna(subset=["person_id", "date_of_death"])
    df["date_of_death"] = pd.to_datetime(df["date_of_death"], errors="coerce")
    return df


# -------------------------------------------------------------------------
# Merge / Business logic
# -------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=30, timeout_seconds=300)
def merge_datasets(
    city_df: pd.DataFrame,
    nuclear_df: pd.DataFrame,
    thermal_df: pd.DataFrame,
    death_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge all source datasets into a single analytical view.

    The merge strategy is:
        1. Join city data with death records on city_code.
        2. Append power plant information (nuclear + thermal) as separate
           reference tables; for this example we simply concatenate them
           into a single ``plants`` DataFrame and attach it as a JSON column.

    Args:
        city_df: Cleaned city geographic data.
        nuclear_df: Cleaned nuclear plant data.
        thermal_df: Cleaned thermal plant data.
        death_df: Cleaned death records.

    Returns:
        DataFrame ready for loading into PostgreSQL.
    """
    logger = get_run_logger()
    logger.info("Merging datasets")

    # Merge city with death records
    merged = death_df.merge(
        city_df,
        left_on="city_code",
        right_on="city_code",
        how="left",
        suffixes=("", "_city"),
    )

    # Combine plant datasets
    plants = pd.concat([nuclear_df, thermal_df], ignore_index=True, sort=False)
    plants_json = plants.to_dict(orient="records")

    # Attach plants JSON as a column (denormalized for illustration)
    merged["plants"] = json.dumps(plants_json)

    logger.info("Merged dataset contains %d rows", len(merged))
    return merged


# -------------------------------------------------------------------------
# Conditional branching task
# -------------------------------------------------------------------------

@task
def should_load_to_postgres(df: pd.DataFrame) -> bool:
    """
    Determine whether the merged DataFrame should be loaded into PostgreSQL.

    The rule is simple: load only if there is at least one row.

    Args:
        df: Merged DataFrame.

    Returns:
        True if loading should proceed, False otherwise.
    """
    logger = get_run_logger()
    row_count = len(df)
    logger.info("Merged DataFrame row count: %d", row_count)
    return row_count > 0


# -------------------------------------------------------------------------
# Main flow definition
# -------------------------------------------------------------------------

@flow(
    name="global_dag",
    description=(
        "ETL pipeline that processes French government death records and power plant "
        "data using a staged ETL pattern with fan‑out/fan‑in parallelism, conditional "
        "branching, and PostgreSQL loading."
    ),
    task_runner=ConcurrentTaskRunner(),
)
def global_dag() -> None:
    """
    Orchestrates the full ETL process.

    Execution steps:
        1. Parallel extraction of all source datasets.
        2. Parallel cleaning of each extracted dataset.
        3. Fan‑in merge of cleaned datasets.
        4. Conditional branching: load only if merged data is non‑empty.
        5. Load merged data into PostgreSQL.
    """
    logger = get_run_logger()
    logger.info("Starting global_dag flow")

    # -----------------------------------------------------------------
    # Extraction (fan‑out)
    # -----------------------------------------------------------------
    city_raw = extract_city_geo_data.submit()
    nuclear_raw = extract_nuclear_plants_data.submit()
    thermal_raw = extract_thermal_plants_data.submit()
    death_raw = extract_death_records_data.submit()

    # -----------------------------------------------------------------
    # Cleaning (fan‑out)
    # -----------------------------------------------------------------
    city_clean = clean_city_geo.submit(city_raw)
    nuclear_clean = clean_nuclear_plants.submit(nuclear_raw)
    thermal_clean = clean_thermal_plants.submit(thermal_raw)
    death_clean = clean_death_records.submit(death_raw)

    # -----------------------------------------------------------------
    # Merge (fan‑in)
    # -----------------------------------------------------------------
    merged_df = merge_datasets.submit(
        city_df=city_clean,
        nuclear_df=nuclear_clean,
        thermal_df=thermal_clean,
        death_df=death_clean,
    )

    # -----------------------------------------------------------------
    # Conditional branching
    # -----------------------------------------------------------------
    load_flag = should_load_to_postgres.submit(merged_df)

    # -----------------------------------------------------------------
    # Load to PostgreSQL (only if flag is True)
    # -----------------------------------------------------------------
    @task(retries=3, retry_delay_seconds=60, timeout_seconds=600)
    def conditional_load(flag: bool, df: pd.DataFrame) -> None:
        """
        Load data into PostgreSQL if ``flag`` is True.

        Args:
            flag: Result of ``should_load_to_postgres``.
            df: Merged DataFrame.
        """
        logger = get_run_logger()
        if not flag:
            logger.warning("Skipping PostgreSQL load because merged DataFrame is empty")
            return

        # Build SQLAlchemy engine from secret
        secret = _load_secret(POSTGRES_DB_SECRET)
        conn_str = secret.get()
        engine = create_engine(conn_str)

        load_dataframe_to_postgres.run(
            df=df,
            table_name="public.death_records_analytics",
            engine=engine,
            if_exists="replace",
        )
        logger.info("Data successfully loaded into PostgreSQL")

    conditional_load.submit(load_flag, merged_df)

    logger.info("global_dag flow completed")


if __name__ == "__main__":
    # Running the flow directly for debugging / local execution
    global_dag()