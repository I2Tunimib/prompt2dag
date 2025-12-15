from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dagster import (
    In,
    Out,
    RetryPolicy,
    ResourceDefinition,
    OpExecutionContext,
    op,
    job,
    ConfigurableResource,
    InitResourceContext,
)


class PostgresResource(ConfigurableResource):
    """Minimal stub for a PostgreSQL connection."""

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        # In a real implementation, this would execute the query against PostgreSQL.
        print(f"[Postgres] Executing query: {query}")
        if params:
            print(f"[Postgres] With params: {params}")


class RedisResource(ConfigurableResource):
    """Minimal stub for a Redis connection."""

    def set(self, key: str, value: Any) -> None:
        print(f"[Redis] Setting key {key} with value of type {type(value)}")

    def get(self, key: str) -> Any:
        print(f"[Redis] Getting key {key}")
        return None


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch nuclear power plant metadata JSON.",
)
def fetch_nuclear_metadata(context: OpExecutionContext) -> Dict[str, Any]:
    url = "https://example.com/nuclear_metadata.json"
    context.log.info(f"Fetching nuclear metadata from {url}")
    # Stubbed response
    response = {"download_url": "https://example.com/nuclear_data.csv"}
    return response


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch thermal power plant metadata JSON.",
)
def fetch_thermal_metadata(context: OpExecutionContext) -> Dict[str, Any]:
    url = "https://example.com/thermal_metadata.json"
    context.log.info(f"Fetching thermal metadata from {url}")
    response = {"download_url": "https://example.com/thermal_data.csv"}
    return response


@op(
    out=Out(list),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch death records resource list.",
)
def fetch_death_resource_list(context: OpExecutionContext) -> List[str]:
    url = "https://example.com/death_records_resources.json"
    context.log.info(f"Fetching death records resource list from {url}")
    # Stubbed list of file URLs
    return [
        "https://example.com/death_record_2023_01.csv",
        "https://example.com/death_record_2023_02.csv",
    ]


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description="Fetch city geographic coordinates CSV.",
)
def fetch_city_geo_csv(context: OpExecutionContext) -> str:
    url = "https://example.com/cities_geo.csv"
    context.log.info(f"Fetching city geographic CSV from {url}")
    # Stubbed path to saved CSV
    path = "/tmp/cities_geo.csv"
    return path


@op(
    out=Out(Path),
    description="Download nuclear power plant CSV data.",
)
def download_nuclear_csv(
    context: OpExecutionContext, metadata: Dict[str, Any]
) -> Path:
    url = metadata.get("download_url")
    context.log.info(f"Downloading nuclear CSV from {url}")
    # Stubbed download
    path = Path("/tmp/nuclear_data.csv")
    path.write_text("id,name,capacity\n1,Nuclear Plant A,1000")
    return path


@op(
    out=Out(Path),
    description="Download thermal power plant CSV data.",
)
def download_thermal_csv(
    context: OpExecutionContext, metadata: Dict[str, Any]
) -> Path:
    url = metadata.get("download_url")
    context.log.info(f"Downloading thermal CSV from {url}")
    path = Path("/tmp/thermal_data.csv")
    path.write_text("id,name,capacity\n10,Thermal Plant X,500")
    return path


@op(
    out=Out(List[Path]),
    description="Download death record files.",
)
def download_death_files(
    context: OpExecutionContext, resource_urls: List[str]
) -> List[Path]:
    downloaded = []
    for url in resource_urls:
        context.log.info(f"Downloading death record file from {url}")
        filename = Path("/tmp") / Path(url).name
        filename.write_text("id,death_date,city_code\n1001,2023-01-15,12345")
        downloaded.append(filename)
    return downloaded


@op(
    required_resource_keys={"postgres"},
    description="Create deaths table in PostgreSQL.",
)
def create_deaths_table(context: OpExecutionContext) -> None:
    query = """
    CREATE TABLE IF NOT EXISTS deaths (
        id BIGINT PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR(10)
    );
    """
    context.resources.postgres.execute(query)


@op(
    required_resource_keys={"postgres"},
    description="Create power_plants table in PostgreSQL.",
)
def create_power_plants_table(context: OpExecutionContext) -> None:
    query = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id BIGINT PRIMARY KEY,
        name TEXT,
        capacity INTEGER,
        type VARCHAR(10)
    );
    """
    context.resources.postgres.execute(query)


@op(
    required_resource_keys={"redis"},
    description="Load death records into Redis for intermediate storage.",
)
def load_death_records_to_redis(
    context: OpExecutionContext, death_files: List[Path]
) -> List[Dict[str, Any]]:
    records = []
    for file_path in death_files:
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            record = row.to_dict()
            key = f"death:{record['id']}"
            context.resources.redis.set(key, json.dumps(record))
            records.append(record)
    context.log.info(f"Loaded {len(records)} death records into Redis")
    return records


@op(
    description="Clean thermal plant data (renaming columns, filtering).",
)
def clean_thermal_plant_data(
    context: OpExecutionContext, thermal_csv: Path
) -> pd.DataFrame:
    df = pd.read_csv(thermal_csv)
    df = df.rename(columns={"capacity": "plant_capacity"})
    df = df[df["plant_capacity"] > 0]
    context.log.info(f"Cleaned thermal plant data with {len(df)} rows")
    return df


@op(
    description="Clean nuclear plant data (renaming columns, filtering).",
)
def clean_nuclear_plant_data(
    context: OpExecutionContext, nuclear_csv: Path
) -> pd.DataFrame:
    df = pd.read_csv(nuclear_csv)
    df = df.rename(columns={"capacity": "plant_capacity"})
    df = df[df["plant_capacity"] > 0]
    context.log.info(f"Cleaned nuclear plant data with {len(df)} rows")
    return df


@op(
    out=Out(bool),
    description="Check if death data exists in Redis.",
)
def check_death_data_availability(
    context: OpExecutionContext, death_records: List[Dict[str, Any]]
) -> bool:
    has_data = len(death_records) > 0
    context.log.info(f"Death data availability: {has_data}")
    return has_data


@op(
    required_resource_keys={"postgres"},
    description="Cleanse death records (date formatting, geographic enrichment).",
)
def cleanse_death_records(
    context: OpExecutionContext,
    has_data: bool,
    death_records: List[Dict[str, Any]],
    city_geo_path: str,
) -> List[Dict[str, Any]]:
    if not has_data:
        context.log.info("No death data to cleanse; skipping.")
        return []

    # Load city geographic data for enrichment (stubbed)
    city_df = pd.read_csv(city_geo_path) if Path(city_geo_path).exists() else pd.DataFrame()
    enriched = []
    for record in death_records:
        # Date formatting
        record["death_date"] = pd.to_datetime(record["death_date"]).date()
        # Geographic enrichment (stubbed)
        record["city_name"] = city_df.loc[
            city_df["city_code"] == record["city_code"], "city_name"
        ].squeeze() if not city_df.empty else None
        enriched.append(record)
    context.log.info(f"Cleansed {len(enriched)} death records")
    return enriched


@op(
    required_resource_keys={"postgres"},
    description="Store cleansed death records in PostgreSQL.",
)
def store_death_records_pg(
    context: OpExecutionContext, cleansed_records: List[Dict[str, Any]]
) -> None:
    if not cleansed_records:
        context.log.info("No cleansed death records to store; skipping.")
        return
    for rec in cleansed_records:
        query = """
        INSERT INTO deaths (id, death_date, city_code)
        VALUES (%(id)s, %(death_date)s, %(city_code)s)
        ON CONFLICT (id) DO NOTHING;
        """
        context.resources.postgres.execute(query, rec)
    context.log.info(f"Stored {len(cleansed_records)} death records in PostgreSQL")


@op(
    description="Clean temporary death data files.",
)
def clean_temp_death_files(
    context: OpExecutionContext, death_files: List[Path]
) -> None:
    for file_path in death_files:
        try:
            file_path.unlink()
            context.log.info(f"Deleted temporary file {file_path}")
        except FileNotFoundError:
            continue


@op(
    description="Generate SQL queries for power plant data persistence.",
)
def generate_power_plant_sql(
    context: OpExecutionContext,
    thermal_df: pd.DataFrame,
    nuclear_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    records = []
    for _, row in pd.concat([thermal_df, nuclear_df], ignore_index=True).iterrows():
        record = {
            "id": int(row["id"]),
            "name": row["name"],
            "capacity": int(row["plant_capacity"]),
            "type": "thermal" if row["id"] in thermal_df["id"].values else "nuclear",
        }
        records.append(record)
    context.log.info(f"Generated SQL payload for {len(records)} power plant records")
    return records


@op(
    required_resource_keys={"postgres"},
    description="Store power plant records in PostgreSQL.",
)
def store_power_plant_records_pg(
    context: OpExecutionContext, plant_records: List[Dict[str, Any]]
) -> None:
    for rec in plant_records:
        query = """
        INSERT INTO power_plants (id, name, capacity, type)
        VALUES (%(id)s, %(name)s, %(capacity)s, %(type)s)
        ON CONFLICT (id) DO NOTHING;
        """
        context.resources.postgres.execute(query, rec)
    context.log.info(f"Stored {len(plant_records)} power plant records in PostgreSQL")


@op(
    description="Mark staging phase as complete.",
)
def complete_staging(context: OpExecutionContext) -> None:
    context.log.info("Staging phase completed successfully.")


@job(
    description="ETL pipeline for French death records and power plant data.",
    resource_defs={
        "postgres": PostgresResource(),
        "redis": RedisResource(),
    },
)
def etl_pipeline():
    # Parallel ingestion
    nuclear_meta = fetch_nuclear_metadata()
    thermal_meta = fetch_thermal_metadata()
    death_resource_list = fetch_death_resource_list()
    city_geo_path = fetch_city_geo_csv()

    # Process fetched metadata
    nuclear_csv = download_nuclear_csv(nuclear_meta)
    thermal_csv = download_thermal_csv(thermal_meta)
    death_files = download_death_files(death_resource_list)

    # Table creation
    create_deaths_table()
    create_power_plants_table()

    # Parallel data processing
    death_records = load_death_records_to_redis(death_files)
    thermal_clean = clean_thermal_plant_data(thermal_csv)
    nuclear_clean = clean_nuclear_plant_data(nuclear_csv)

    # Branching based on death data availability
    has_death_data = check_death_data_availability(death_records)

    cleansed_deaths = cleanse_death_records(
        has_death_data, death_records, city_geo_path
    )
    store_death_records_pg(cleansed_deaths)
    clean_temp_death_files(death_files)

    # Power plant data processing
    plant_sql_payload = generate_power_plant_sql(thermal_clean, nuclear_clean)
    store_power_plant_records_pg(plant_sql_payload)

    # Completion
    complete_staging()


if __name__ == "__main__":
    result = etl_pipeline.execute_in_process()
    if result.success:
        print("ETL pipeline executed successfully.")
    else:
        print("ETL pipeline failed.")