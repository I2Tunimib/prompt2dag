from prefect import flow, task
import time
import os
from typing import List, Dict, Any


@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_power_plant_metadata_json() -> Dict[str, Any]:
    """Fetch nuclear power plant metadata from French government API."""
    time.sleep(1)
    return {"download_url": "https://data.gouv.fr/nuclear.csv", "record_count": 50}


@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_power_plant_metadata_json() -> Dict[str, Any]:
    """Fetch thermal power plant metadata from French government API."""
    time.sleep(1)
    return {"download_url": "https://data.gouv.fr/thermal.csv", "record_count": 30}


@task(retries=3, retry_delay_seconds=10)
def fetch_death_records_resource_list() -> List[str]:
    """Fetch death records resource list from national registry."""
    time.sleep(1)
    return ["https://data.gouv.fr/deaths_2023.csv", "https://data.gouv.fr/deaths_2024.csv"]


@task(retries=3, retry_delay_seconds=10)
def fetch_city_geographic_coordinates_csv() -> str:
    """Fetch city geographic coordinates CSV."""
    time.sleep(1)
    return "https://data.gouv.fr/cities.csv"


@task(retries=3, retry_delay_seconds=10)
def download_nuclear_power_plant_csv_data(metadata: Dict[str, Any]) -> str:
    """Download nuclear power plant CSV data."""
    time.sleep(1)
    return "/tmp/nuclear_plants_raw.csv"


@task(retries=3, retry_delay_seconds=10)
def download_thermal_power_plant_csv_data(metadata: Dict[str, Any]) -> str:
    """Download thermal power plant CSV data."""
    time.sleep(1)
    return "/tmp/thermal_plants_raw.csv"


@task(retries=3, retry_delay_seconds=10)
def download_death_record_files(resource_list: List[str]) -> List[str]:
    """Download death record files."""
    time.sleep(2)
    return [f"/tmp/deaths_{i}.csv" for i in range(len(resource_list))]


@task(retries=3, retry_delay_seconds=10)
def create_deaths_table():
    """Create PostgreSQL deaths table."""
    time.sleep(1)
    print("Created deaths table in PostgreSQL")


@task(retries=3, retry_delay_seconds=10)
def create_power_plants_table():
    """Create PostgreSQL power_plants table."""
    time.sleep(1)
    print("Created power_plants table in PostgreSQL")


@task(retries=3, retry_delay_seconds=10)
def load_death_records_into_redis(death_files: List[str]) -> bool:
    """Load death records from ingestion files into Redis."""
    time.sleep(1)
    return len(death_files) > 0


@task(retries=3, retry_delay_seconds=10)
def clean_thermal_plant_data(csv_path: str) -> str:
    """Clean thermal plant data (column renaming, filtering)."""
    time.sleep(1)
    return "/tmp/thermal_plants_clean.csv"


@task(retries=3, retry_delay_seconds=10)
def clean_nuclear_plant_data(csv_path: str) -> str:
    """Clean nuclear plant data (column renaming, filtering)."""
    time.sleep(1)
    return "/tmp/nuclear_plants_clean.csv"


@task(retries=3, retry_delay_seconds=10)
def cleanse_death_records(death_files: List[str]) -> str:
    """Cleanse death records (date formatting, geographic enrichment)."""
    time.sleep(2)
    return "/tmp/deaths_clean.csv"


@task(retries=3, retry_delay_seconds=10)
def store_cleansed_death_records_in_postgresql(cleaned_file: str):
    """Store cleansed death records in PostgreSQL."""
    time.sleep(1)
    print(f"Stored death records from {cleaned_file} to PostgreSQL")


@task(retries=3, retry_delay_seconds=10)
def clean_temporary_death_data_files(death_files: List[str]):
    """Clean temporary death data files."""
    time.sleep(1)
    for file_path in death_files:
        if os.path.exists(file_path):
            os.remove(file_path)
    print("Cleaned temporary death data files")


@task(retries=3, retry_delay_seconds=10)
def generate_sql_queries_for_plant_data(thermal_clean: str, nuclear_clean: str) -> str:
    """Generate SQL queries for plant data persistence."""
    time.sleep(1)
    return "/tmp/plant_inserts.sql"


@task(retries=3, retry_delay_seconds=10)
def store_power_plant_records_in_postgresql(sql_file: str):
    """Store power plant records in PostgreSQL."""
    time.sleep(1)
    print(f"Stored power plant records from {sql_file} to PostgreSQL")


@task(retries=3, retry_delay_seconds=10)
def complete_staging_phase():
    """Complete staging phase."""
    time.sleep(1)
    print("Staging phase completed successfully")


@flow(name="french-government-etl-pipeline")
def french_government_etl_pipeline():
    """ETL pipeline for French government death records and power plant data."""
    
    # Ingestion Phase - Fan-out parallelism
    nuclear_meta_future = fetch_nuclear_power_plant_metadata_json.submit()
    thermal_meta_future = fetch_thermal_power_plant_metadata_json.submit()
    death_resources_future = fetch_death_records_resource_list.submit()
    city_coords_future = fetch_city_geographic_coordinates_csv.submit()
    
    # Fan-in synchronization after ingestion
    nuclear_meta = nuclear_meta_future.result()
    thermal_meta = thermal_meta_future.result()
    death_resources = death_resources_future.result()
    city_coords = city_coords_future.result()
    
    # Process fetched metadata
    nuclear_csv_future = download_nuclear_power_plant_csv_data.submit(nuclear_meta)
    thermal_csv_future = download_thermal_power_plant_csv_data.submit(thermal_meta)
    death_files_future = download_death_record_files.submit(death_resources)
    
    nuclear_csv = nuclear_csv_future.result()
    thermal_csv = thermal_csv_future.result()
    death_files = death_files_future.result()
    
    # Create PostgreSQL tables - parallel execution
    deaths_table_future = create_deaths_table.submit()
    power_plants_table_future = create_power_plants_table.submit()
    
    deaths_table_future.result()
    power_plants_table_future.result()
    
    # Parallel data processing
    death_data_exists_future = load_death_records_into_redis.submit(death_files)
    thermal_clean_future = clean_thermal_plant_data.submit(thermal_csv)
    nuclear_clean_future = clean_nuclear_plant_data.submit(nuclear_csv)
    
    death_data_exists = death_data_exists_future.result()
    thermal_clean = thermal_clean_future.result()
    nuclear_clean = nuclear_clean_future.result()
    
    # Conditional branching based on death data availability
    if death_data_exists:
        cleaned_deaths = cleanse_death_records(death_files)
        store_cleansed_death_records_in_postgresql(cleaned_deaths)
        clean_temporary_death_data_files(death_files)
    else:
        print("No death data available, skipping death record processing")
    
    # Power plant data processing
    sql_queries = generate_sql_queries_for_plant_data(thermal_clean, nuclear_clean)
    store_power_plant_records_in_postgresql(sql_queries)
    
    # Complete staging phase
    complete_staging_phase()


if __name__ == '__main__':
    # Manual trigger only, no schedule
    # Maximum concurrency of 1 is configured at deployment time
    french_government_etl_pipeline()