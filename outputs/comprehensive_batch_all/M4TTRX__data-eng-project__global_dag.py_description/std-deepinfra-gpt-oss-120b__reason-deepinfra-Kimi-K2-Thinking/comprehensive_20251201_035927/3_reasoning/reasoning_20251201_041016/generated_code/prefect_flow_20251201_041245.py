from prefect import flow, task
import time
from typing import List, Dict, Any


# Mock external dependencies - replace with actual implementations
class MockAPIClient:
    @staticmethod
    def fetch_json(url: str) -> Dict[str, Any]:
        time.sleep(1)
        return {"url": url, "csv_link": f"{url}/download"}
    
    @staticmethod
    def fetch_csv(url: str) -> str:
        time.sleep(1)
        return f"csv_content_from_{url}"

class MockDatabase:
    @staticmethod
    def execute(query: str, params: tuple = None):
        time.sleep(0.5)
        print(f"Executed: {query}")

class MockRedis:
    @staticmethod
    def load_data(files: List[str]) -> bool:
        time.sleep(1)
        return len(files) > 0


@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_metadata() -> Dict[str, Any]:
    """Fetch nuclear power plant metadata JSON from French government API."""
    client = MockAPIClient()
    return client.fetch_json("https://data.gouv.fr/api/nuclear/metadata")


@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_metadata() -> Dict[str, Any]:
    """Fetch thermal power plant metadata JSON from French government API."""
    client = MockAPIClient()
    return client.fetch_json("https://data.gouv.fr/api/thermal/metadata")


@task(retries=3, retry_delay_seconds=10)
def fetch_death_records_resource_list() -> List[str]:
    """Fetch death records resource list from national registry."""
    client = MockAPIClient()
    data = client.fetch_json("https://data.gouv.fr/api/death-records")
    return [f"resource_{i}" for i in range(3)]


@task(retries=3, retry_delay_seconds=10)
def fetch_city_geographic_coordinates() -> str:
    """Fetch city geographic coordinates CSV."""
    client = MockAPIClient()
    return client.fetch_csv("https://data.gouv.fr/api/cities/coordinates.csv")


@task(retries=3, retry_delay_seconds=10)
def download_nuclear_csv(metadata: Dict[str, Any]) -> str:
    """Download nuclear power plant CSV data based on metadata."""
    csv_url = metadata.get("csv_link", "")
    client = MockAPIClient()
    return client.fetch_csv(csv_url)


@task(retries=3, retry_delay_seconds=10)
def download_thermal_csv(metadata: Dict[str, Any]) -> str:
    """Download thermal power plant CSV data based on metadata."""
    csv_url = metadata.get("csv_link", "")
    client = MockAPIClient()
    return client.fetch_csv(csv_url)


@task(retries=3, retry_delay_seconds=10)
def download_death_record_files(resource_list: List[str]) -> List[str]:
    """Download death record files from resource list."""
    downloaded_files = []
    for resource in resource_list:
        time.sleep(0.5)
        downloaded_files.append(f"/tmp/{resource}.csv")
    return downloaded_files


@task(retries=3, retry_delay_seconds=10)
def create_deaths_table():
    """Create PostgreSQL table for death records."""
    db = MockDatabase()
    db.execute("""
        CREATE TABLE IF NOT EXISTS deaths (
            id SERIAL PRIMARY KEY,
            record_date DATE,
            city_insee_code VARCHAR(10),
            death_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@task(retries=3, retry_delay_seconds=10)
def create_power_plants_table():
    """Create PostgreSQL table for power plant records."""
    db = MockDatabase()
    db.execute("""
        CREATE TABLE IF NOT EXISTS power_plants (
            id SERIAL PRIMARY KEY,
            plant_type VARCHAR(50),
            plant_name VARCHAR(255),
            capacity_mw DECIMAL(10,2),
            city_insee_code VARCHAR(10),
            operational_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@task(retries=3, retry_delay_seconds=10)
def load_death_records_to_redis(death_files: List[str]) -> bool:
    """Load death records from ingestion files into Redis."""
    if not death_files:
        return False
    redis = MockRedis()
    return redis.load_data(death_files)


@task(retries=3, retry_delay_seconds=10)
def clean_thermal_plant_data(csv_data: str) -> Dict[str, Any]:
    """Clean thermal plant data (column renaming, filtering)."""
    time.sleep(1)
    return {
        "plant_type": "thermal",
        "cleaned_data": f"cleaned_{csv_data}",
        "record_count": 150
    }


@task(retries=3, retry_delay_seconds=10)
def clean_nuclear_plant_data(csv_data: str) -> Dict[str, Any]:
    """Clean nuclear plant data (column renaming, filtering)."""
    time.sleep(1)
    return {
        "plant_type": "nuclear",
        "cleaned_data": f"cleaned_{csv_data}",
        "record_count": 58
    }


@task(retries=3, retry_delay_seconds=10)
def check_death_data_availability(redis_load_result: bool) -> bool:
    """Check if death data is available in Redis."""
    return redis_load_result


@task(retries=3, retry_delay_seconds=10)
def cleanse_death_records(city_coords: str) -> List[Dict[str, Any]]:
    """Cleanse death records (date formatting, geographic enrichment)."""
    time.sleep(1.5)
    print(f"Enriching death records with city coordinates: {city_coords[:50]}...")
    return [
        {"record_date": "2023-01-01", "city_insee_code": "75056", "death_count": 45},
        {"record_date": "2023-01-02", "city_insee_code": "75056", "death_count": 38},
    ]


@task(retries=3, retry_delay_seconds=10)
def store_cleansed_death_records(cleansed_data: List[Dict[str, Any]]):
    """Store cleansed death records in PostgreSQL."""
    db = MockDatabase()
    for record in cleansed_data:
        db.execute(
            "INSERT INTO deaths (record_date, city_insee_code, death_count) VALUES (%s, %s, %s)",
            (record["record_date"], record["city_insee_code"], record["death_count"])
        )
    print(f"Stored {len(cleansed_data)} death records to PostgreSQL")


@task(retries=3, retry_delay_seconds=10)
def clean_temporary_death_files(death_files: List[str]):
    """Clean temporary death data files."""
    for file_path in death_files:
        print(f"Deleted temporary file: {file_path}")


@task(retries=3, retry_delay_seconds=10)
def generate_plant_sql_queries(cleaned_thermal: Dict[str, Any], cleaned_nuclear: Dict[str, Any]) -> str:
    """Generate SQL queries for plant data persistence."""
    time.sleep(1)
    return f"sql_queries_from_{cleaned_thermal['plant_type']}_and_{cleaned_nuclear['plant_type']}"


@task(retries=3, retry_delay_seconds=10)
def store_power_plant_records(sql_queries: str):
    """Store power plant records in PostgreSQL."""
    db = MockDatabase()
    db.execute(sql_queries)
    print("Stored power plant records in PostgreSQL")


@flow(name="french-government-etl-pipeline")
def french_government_etl_pipeline():
    """
    ETL pipeline for processing French government death records and power plant data.
    
    Manual trigger only. For deployment with max concurrency of 1, configure:
    prefect deployment build ... --limit 1
    """
    
    # Phase 1: Parallel data ingestion (fan-out)
    nuclear_metadata_future = fetch_nuclear_metadata.submit()
    thermal_metadata_future = fetch_thermal_metadata.submit()
    death_resources_future = fetch_death_records_resource_list.submit()
    city_coords_future = fetch_city_geographic_coordinates.submit()
    
    # Wait for ingestion results
    nuclear_metadata = nuclear_metadata_future.result()
    thermal_metadata = thermal_metadata_future.result()
    death_resources = death_resources_future.result()
    city_coords = city_coords_future.result()
    
    # Phase 2: Download data files (parallel)
    nuclear_csv_future = download_nuclear_csv.submit(nuclear_metadata)
    thermal_csv_future = download_thermal_csv.submit(thermal_metadata)
    death_files_future = download_death_record_files.submit(death_resources)
    
    # Wait for download results
    nuclear_csv = nuclear_csv_future.result()
    thermal_csv = thermal_csv_future.result()
    death_files = death_files_future.result()
    
    # Phase 3: Create PostgreSQL tables (parallel)
    create_deaths_table_future = create_deaths_table.submit()
    create_power_plants_table_future = create_power_plants_table.submit()
    
    # Wait for table creation
    create_deaths_table_future.wait()
    create_power_plants_table_future.wait()
    
    # Phase 4: Parallel data processing
    redis_load_future = load_death_records_to_redis.submit(death_files)
    clean_thermal_future = clean_thermal_plant_data.submit(thermal_csv)
    clean_nuclear_future = clean_nuclear_plant_data.submit(nuclear_csv)
    
    # Wait for processing results
    redis_load_result = redis_load_future.result()
    cleaned_thermal = clean_thermal_future.result()
    cleaned_nuclear = clean_nuclear_future.result()
    
    # Phase 5: Conditional branching based on death data availability
    death_data_available = check_death_data_availability(redis_load_result)
    
    if death_data_available:
        # Death data processing path
        cleansed_deaths = cleanse_death_records(city_coords)
        store_cleansed_death_records(cleansed_deaths)
        clean_temporary_death_files(death_files)