from __future__ import annotations

import csv
import io
import json
import time
from typing import Any, Dict, List, Optional

import requests
from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    In,
    Out,
    RetryPolicy,
    ResourceDefinition,
    String,
    op,
    job,
    ConfigurableResource,
)


class PostgresResource(ConfigurableResource):
    """Minimal stub for a PostgreSQL connection."""

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        # In a real implementation, this would execute the query against PostgreSQL.
        # Here we just log the query for demonstration purposes.
        print(f"[Postgres] Executing query: {query}")
        if params:
            print(f"[Postgres] With params: {params}")


class RedisResource(ConfigurableResource):
    """Minimal stub for a Redis connection."""

    _store: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        print(f"[Redis] Set key={key}")

    def get(self, key: str) -> Any:
        value = self._store.get(key)
        print(f"[Redis] Get key={key} -> {value}")
        return value

    def delete(self, key: str) -> None:
        if key in self._store:
            del self._store[key]
            print(f"[Redis] Deleted key={key}")


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(dict),
    description="Fetch nuclear power plant metadata JSON.",
)
def fetch_nuclear_metadata(context) -> Dict[str, Any]:
    url = "https://example.com/nuclear_metadata.json"
    context.log.info(f"Fetching nuclear metadata from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(dict),
    description="Fetch thermal power plant metadata JSON.",
)
def fetch_thermal_metadata(context) -> Dict[str, Any]:
    url = "https://example.com/thermal_metadata.json"
    context.log.info(f"Fetching thermal metadata from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(list),
    description="Fetch death records resource list.",
)
def fetch_death_resource_list(context) -> List[str]:
    url = "https://example.com/death_records_resources.json"
    context.log.info(f"Fetching death records resource list from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    # Assume the list of file URLs is under the key "files"
    return data.get("files", [])


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(list),
    description="Fetch city geographic coordinates CSV.",
)
def fetch_city_coords(context) -> List[Dict[str, Any]]:
    url = "https://example.com/city_coords.csv"
    context.log.info(f"Fetching city coordinates CSV from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    text = response.text
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"metadata": In(dict)},
    out=Out(list),
    description="Download nuclear power plant CSV data based on metadata.",
)
def download_nuclear_csv(context, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    csv_url = metadata.get("csv_url", "https://example.com/nuclear_data.csv")
    context.log.info(f"Downloading nuclear CSV from {csv_url}")
    response = requests.get(csv_url, timeout=10)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"metadata": In(dict)},
    out=Out(list),
    description="Download thermal power plant CSV data based on metadata.",
)
def download_thermal_csv(context, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    csv_url = metadata.get("csv_url", "https://example.com/thermal_data.csv")
    context.log.info(f"Downloading thermal CSV from {csv_url}")
    response = requests.get(csv_url, timeout=10)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"file_urls": In(list)},
    out=Out(list),
    description="Download death record files.",
)
def download_death_files(context, file_urls: List[str]) -> List[Dict[str, Any]]:
    records = []
    for url in file_urls:
        context.log.info(f"Downloading death record file from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        # Assume each file is a JSON array of records
        records.extend(response.json())
    return records


@op(
    required_resource_keys={"postgres"},
    description="Create deaths table in PostgreSQL.",
)
def create_deaths_table(context) -> None:
    query = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR,
        other_fields JSONB
    );
    """
    context.resources.postgres.execute(query)


@op(
    required_resource_keys={"postgres"},
    description="Create power_plants table in PostgreSQL.",
)
def create_power_plants_table(context) -> None:
    query = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_type VARCHAR,
        name VARCHAR,
        capacity_mw NUMERIC,
        city_code VARCHAR,
        other_fields JSONB
    );
    """
    context.resources.postgres.execute(query)


@op(
    required_resource_keys={"redis"},
    ins={"death_records": In(list)},
    out=Out(bool),
    description="Load death records into Redis for temporary storage.",
)
def load_death_records_to_redis(context, death_records: List[Dict[str, Any]]) -> bool:
    key = "death_records"
    context.resources.redis.set(key, json.dumps(death_records))
    has_data = len(death_records) > 0
    context.log.info(f"Loaded {len(death_records)} death records into Redis. Data present: {has_data}")
    return has_data


@op(
    out=Out(list),
    description="Clean thermal plant data (rename columns, filter).",
)
def clean_thermal_plant_data(context, thermal_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned = []
    for row in thermal_data:
        # Example renaming and filtering logic
        cleaned_row = {
            "name": row.get("PlantName"),
            "capacity_mw": float(row.get("CapacityMW", 0)),
            "city_code": row.get("INSEE"),
            "plant_type": "thermal",
            "other_fields": json.dumps(row),
        }
        if cleaned_row["capacity_mw"] > 0:
            cleaned.append(cleaned_row)
    context.log.info(f"Cleaned thermal data: {len(cleaned)} records")
    return cleaned


@op(
    out=Out(list),
    description="Clean nuclear plant data (rename columns, filter).",
)
def clean_nuclear_plant_data(context, nuclear_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned = []
    for row in nuclear_data:
        cleaned_row = {
            "name": row.get("Nom"),
            "capacity_mw": float(row.get("PuissanceMW", 0)),
            "city_code": row.get("INSEE"),
            "plant_type": "nuclear",
            "other_fields": json.dumps(row),
        }
        if cleaned_row["capacity_mw"] > 0:
            cleaned.append(cleaned_row)
    context.log.info(f"Cleaned nuclear data: {len(cleaned)} records")
    return cleaned


@op(
    ins={"has_data": In(bool)},
    out=Out(list),
    description="Cleanse death records if data exists; otherwise return empty list.",
)
def cleanse_death_records(context, has_data: bool) -> List[Dict[str, Any]]:
    if not has_data:
        context.log.info("No death data available; skipping cleansing.")
        return []
    # Retrieve raw data from Redis
    raw_json = context.resources.redis.get("death_records")
    raw_records = json.loads(raw_json) if raw_json else []
    cleaned = []
    for rec in raw_records:
        cleaned_rec = {
            "death_date": rec.get("date_deces"),
            "city_code": rec.get("insee_commune"),
            "other_fields": json.dumps(rec),
        }
        cleaned.append(cleaned_rec)
    context.log.info(f"Cleansed death records: {len(cleaned)} records")
    return cleaned


@op(
    required_resource_keys={"postgres"},
    ins={"cleaned_deaths": In(list)},
    description="Store cleansed death records in PostgreSQL.",
)
def store_death_records_pg(context, cleaned_deaths: List[Dict[str, Any]]) -> None:
    if not cleaned_deaths:
        context.log.info("No death records to store.")
        return
    for rec in cleaned_deaths:
        query = """
        INSERT INTO deaths (death_date, city_code, other_fields)
        VALUES (%s, %s, %s);
        """
        params = (
            rec["death_date"],
            rec["city_code"],
            rec["other_fields"],
        )
        context.resources.postgres.execute(query, {"params": params})
    context.log.info(f"Stored {len(cleaned_deaths)} death records in PostgreSQL.")


@op(
    description="Clean temporary death data files (remove Redis key).",
)
def clean_temp_death_files(context) -> None:
    context.resources.redis.delete("death_records")
    context.log.info("Temporary death data cleaned from Redis.")


@op(
    out=DynamicOut(list),
    description="Generate SQL queries for power plant data persistence.",
)
def generate_power_plant_sql(context, thermal_clean: List[Dict[str, Any]], nuclear_clean: List[Dict[str, Any]]) -> DynamicOutput:
    all_plants = thermal_clean + nuclear_clean
    for plant in all_plants:
        query = """
        INSERT INTO power_plants (plant_type, name, capacity_mw, city_code, other_fields)
        VALUES (%s, %s, %s, %s, %s);
        """
        params = (
            plant["plant_type"],
            plant["name"],
            plant["capacity_mw"],
            plant["city_code"],
            plant["other_fields"],
        )
        yield DynamicOutput(value={"query": query, "params": {"params": params}}, mapping_key=plant["name"])


@op(
    required_resource_keys={"postgres"},
    ins={"sql_payloads": In(list)},
    description="Store power plant records in PostgreSQL using generated SQL.",
)
def store_power_plant_records_pg(context, sql_payloads: List[Dict[str, Any]]) -> None:
    for payload in sql_payloads:
        context.resources.postgres.execute(payload["query"], payload["params"])
    context.log.info(f"Stored {len(sql_payloads)} power plant records in PostgreSQL.")


@op(
    description="Finalize staging phase after all data has been processed.",
)
def complete_staging(context) -> None:
    context.log.info("Staging phase completed successfully.")


@job(
    resource_defs={
        "postgres": ResourceDefinition.hardcoded_resource(PostgresResource()),
        "redis": ResourceDefinition.hardcoded_resource(RedisResource()),
    },
    description="ETL pipeline for French government death records and power plant data.",
)
def etl_pipeline():
    # Parallel ingestion
    nuclear_meta = fetch_nuclear_metadata()
    thermal_meta = fetch_thermal_metadata()
    death_resource_list = fetch_death_resource_list()
    city_coords = fetch_city_coords()

    # Download steps
    nuclear_csv = download_nuclear_csv(nuclear_meta)
    thermal_csv = download_thermal_csv(thermal_meta)
    death_files = download_death_files(death_resource_list)

    # Table creation
    create_deaths_table()
    create_power_plants_table()

    # Parallel processing
    death_data_present = load_death_records_to_redis(death_files)
    thermal_clean = clean_thermal_plant_data(thermal_csv)
    nuclear_clean = clean_nuclear_plant_data(nuclear_csv)

    # Death data branch
    cleansed_deaths = cleanse_death_records(death_data_present)
    store_death_records_pg(cleansed_deaths)
    clean_temp_death_files()

    # Power plant processing
    sql_dynamic = generate_power_plant_sql(thermal_clean, nuclear_clean)
    # Collect dynamic outputs into a list for the storing op
    sql_payloads = sql_dynamic.map(lambda x: x)
    store_power_plant_records_pg(sql_payloads)

    # Completion
    complete_staging()


if __name__ == "__main__":
    result = etl_pipeline.execute_in_process()
    if result.success:
        print("ETL pipeline executed successfully.")
    else:
        print("ETL pipeline failed.")