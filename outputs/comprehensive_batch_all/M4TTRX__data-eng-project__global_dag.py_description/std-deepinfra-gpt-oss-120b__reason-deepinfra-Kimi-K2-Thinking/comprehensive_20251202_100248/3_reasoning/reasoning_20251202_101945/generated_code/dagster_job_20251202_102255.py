from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    RetryPolicy,
    ResourceDefinition,
    Nothing,
    Out,
    In,
    get_dagster_logger,
)
import time
from typing import Dict, List, Any, Optional

logger = get_dagster_logger()

# Minimal resource stubs
class PostgresResource:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def execute(self, query: str):
        # Stub implementation
        logger.info(f"Executing PostgreSQL query: {query}")
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]):
        logger.info(f"Inserting {len(data)} records into {table}")

class RedisResource:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
    
    def set(self, key: str, value: Any):
        logger.info(f"Setting Redis key: {key}")
    
    def get(self, key: str) -> Optional[Any]:
        logger.info(f"Getting Redis key: {key}")
        return None

# Configuration classes
class PipelineConfig(Config):
    nuclear_metadata_url: str = "https://example.com/nuclear/metadata"
    thermal_metadata_url: str = "https://example.com/thermal/metadata"
    death_records_url: str = "https://example.com/deaths/list"
    city_coordinates_url: str = "https://example.com/cities/coordinates"
    postgres_connection: str = "postgresql://user:pass@localhost:5432/etl_db"
    redis_host: str = "localhost"
    redis_port: int = 6379

# Ops
@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(dict, description="Nuclear power plant metadata")
)
def fetch_nuclear_metadata(context: OpExecutionContext, config: PipelineConfig):
    """Fetch nuclear power plant metadata JSON from French government API."""
    logger.info("Fetching nuclear power plant metadata...")
    # Simulate API call
    time.sleep(1)
    return {"metadata": "nuclear_plants", "url": config.nuclear_metadata_url}

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(dict, description="Thermal power plant metadata")
)
def fetch_thermal_metadata(context: OpExecutionContext, config: PipelineConfig):
    """Fetch thermal power plant metadata JSON from French government API."""
    logger.info("Fetching thermal power plant metadata...")
    time.sleep(1)
    return {"metadata": "thermal_plants", "url": config.thermal_metadata_url}

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(list, description="Death records resource list")
)
def fetch_death_records_list(context: OpExecutionContext, config: PipelineConfig):
    """Fetch death records resource list from French government API."""
    logger.info("Fetching death records resource list...")
    time.sleep(1)
    return ["death_record_1.csv", "death_record_2.csv"]

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(str, description="City geographic coordinates CSV content")
)
def fetch_city_coordinates(context: OpExecutionContext, config: PipelineConfig):
    """Fetch city geographic coordinates CSV from French government API."""
    logger.info("Fetching city geographic coordinates...")
    time.sleep(1)
    return "city,insee_code,lat,lon\nParis,75056,48.8566,2.3522"

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"metadata": In(dict)},
    out=Out(str, description="Downloaded nuclear CSV data")
)
def download_nuclear_csv(context: OpExecutionContext, metadata: dict):
    """Download nuclear power plant CSV data based on metadata."""
    logger.info(f"Downloading nuclear CSV from {metadata['url']}...")
    time.sleep(1)
    return "nuclear_plant_data.csv"

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"metadata": In(dict)},
    out=Out(str, description="Downloaded thermal CSV data")
)
def download_thermal_csv(context: OpExecutionContext, metadata: dict):
    """Download thermal power plant CSV data based on metadata."""
    logger.info(f"Downloading thermal CSV from {metadata['url']}...")
    time.sleep(1)
    return "thermal_plant_data.csv"

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"resource_list": In(list)},
    out=Out(List[str], description="Downloaded death record files")
)
def download_death_files(context: OpExecutionContext, resource_list: list):
    """Download death record files based on resource list."""
    logger.info(f"Downloading {len(resource_list)} death record files...")
    time.sleep(2)
    return [f"/tmp/{file}" for file in resource_list]

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(Nothing)
)
def create_deaths_table(context: OpExecutionContext, config: PipelineConfig):
    """Create PostgreSQL table for death records."""
    logger.info("Creating deaths table...")
    postgres = PostgresResource(config.postgres_connection)
    postgres.execute("CREATE TABLE IF NOT EXISTS deaths (id SERIAL, date DATE, city VARCHAR, insee_code VARCHAR, lat FLOAT, lon FLOAT)")
    return None

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    out=Out(Nothing)
)
def create_power_plants_table(context: OpExecutionContext, config: PipelineConfig):
    """Create PostgreSQL table for power plant records."""
    logger.info("Creating power_plants table...")
    postgres = PostgresResource(config.postgres_connection)
    postgres.execute("CREATE TABLE IF NOT EXISTS power_plants (id SERIAL, name VARCHAR, type VARCHAR, capacity FLOAT, city VARCHAR)")
    return None

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"death_files": In(List[str])},
    out=Out(bool, description="Whether death data was successfully loaded to Redis")
)
def load_deaths_to_redis(context: OpExecutionContext, death_files: List[str], config: PipelineConfig):
    """Load death records from ingestion files into Redis."""
    logger.info(f"Loading {len(death_files)} death files to Redis...")
    redis = RedisResource(config.redis_host, config.redis_port)
    for file in death_files:
        redis.set(f"death_file:{file}", "loaded")
    # Simulate data availability check
    return len(death_files) > 0

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"csv_data": In(str)},
    out=Out(dict, description="Cleaned thermal plant data")
)
def clean_thermal_data(context: OpExecutionContext, csv_data: str):
    """Clean thermal plant data (column renaming, filtering)."""
    logger.info("Cleaning thermal plant data...")
    time.sleep(1)
    return {"data": csv_data, "cleaned": True, "type": "thermal"}

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"csv_data": In(str)},
    out=Out(dict, description="Cleaned nuclear plant data")
)
def clean_nuclear_data(context: OpExecutionContext, csv_data: str):
    """Clean nuclear plant data (column renaming, filtering)."""
    logger.info("Cleaning nuclear plant data...")
    time.sleep(1)
    return {"data": csv_data, "cleaned": True, "type": "nuclear"}

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"data_loaded": In(bool)},
    out=Out(bool, description="Death data availability flag")
)
def check_death_data_availability(context: OpExecutionContext, data_loaded: bool):
    """Check if death data is available for processing."""
    logger.info(f"Death data available: {data_loaded}")
    return data_loaded

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"data_available": In(bool)},
    out=Out(Optional[List[dict]], description="Cleansed death records")
)
def cleanse_death_records(context: OpExecutionContext, data_available: bool):
    """Cleanse death records (date formatting, geographic enrichment)."""
    if not data_available:
        logger.info("No death data available, skipping cleansing...")
        return None
    
    logger.info("Cleansing death records...")
    time.sleep(2)
    # Simulate cleansing
    return [
        {"date": "2023-01-01", "city": "Paris", "insee_code": "75056", "lat": 48.8566, "lon": 2.3522},
        {"date": "2023-01-02", "city": "Lyon", "insee_code": "69123", "lat": 45.7640, "lon": 4.8357},
    ]

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={
        "cleansed_data": In(Optional[List[dict]]),
        "_table_created": In(Nothing)  # Dependency on table creation
    },
    out=Out(Nothing)
)
def store_cleansed_death_records(context: OpExecutionContext, cleansed_data: Optional[List[dict]], config: PipelineConfig):
    """Store cleansed death records in PostgreSQL."""
    if cleansed_data is None:
        logger.info("No cleansed data to store...")
        return None
    
    logger.info(f"Storing {len(cleansed_data)} death records in PostgreSQL...")
    postgres = PostgresResource(config.postgres_connection)
    postgres.insert_many("deaths", cleansed_data)
    return None

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={"_stored": In(Nothing)},
    out=Out(Nothing)
)
def clean_death_temp_files(context: OpExecutionContext):
    """Clean temporary death data files."""
    logger.info("Cleaning temporary death data files...")
    # Simulate file cleanup
    return None

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={
        "thermal_data": In(dict),
        "nuclear_data": In(dict)
    },
    out=Out(List[str], description="SQL queries for plant data persistence")
)
def generate_plant_sql_queries(context: OpExecutionContext, thermal_data: dict, nuclear_data: dict):
    """Generate SQL queries for plant data persistence."""
    logger.info("Generating SQL queries for power plant data...")
    queries = []
    if thermal_data.get("cleaned"):
        queries.append("INSERT INTO power_plants (name, type, capacity, city) VALUES ('Plant1', 'thermal', 100.5, 'City1')")
    if nuclear_data.get("cleaned"):
        queries.append("INSERT INTO power_plants (name, type, capacity, city) VALUES ('Plant2', 'nuclear', 200.5, 'City2')")
    return queries

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={
        "queries": In(List[str]),
        "_table_created": In(Nothing)  # Dependency on table creation
    },
    out=Out(Nothing)
)
def store_power_plant_records(context: OpExecutionContext, queries: List[str], config: PipelineConfig):
    """Store power plant records in PostgreSQL."""
    logger.info(f"Storing {len(queries)} power plant records in PostgreSQL...")
    postgres = PostgresResource(config.postgres_connection)
    for query in queries:
        postgres.execute(query)
    return None

@op(
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    ins={
        "_death_path": In(Nothing),
        "_plant_path": In(Nothing)
    },
    out=Out(Nothing)
)
def complete_staging(context: OpExecutionContext):
    """Complete the staging phase."""
    logger.info("Staging phase completed successfully!")
    return None

# Job definition
@job(
    description="ETL pipeline for French government death records and power plant data",
    resource_defs={
        "postgres": ResourceDefinition.hardcoded_resource(PostgresResource("postgresql://user:pass@localhost:5432/etl_db")),
        "redis": ResourceDefinition.hardcoded_resource(RedisResource("localhost", 6379)),
    }
)
def french_government_etl_job():
    # Ingestion phase - fan-out
    nuclear_meta = fetch_nuclear_metadata()
    thermal_meta = fetch_thermal_metadata()
    death_list = fetch_death_records_list()
    city_coords = fetch_city_coordinates()  # Not used downstream but part of ingestion
    
    # Download phase - depends on ingestion
    nuclear_csv = download_nuclear_csv(nuclear_meta)
    thermal_csv = download_thermal_csv(thermal_meta)
    death_files = download_death_files(death_list)
    
    # Table creation - can run in parallel with downloads
    deaths_table = create_deaths_table()
    plants_table = create_power_plants_table()
    
    # Processing phase - fan-out
    deaths_loaded = load_deaths_to_redis(death_files)
    thermal_clean = clean_thermal_data(thermal_csv)
    nuclear_clean = clean_nuclear_data(nuclear_csv)
    
    # Branching check
    data_available = check_death_data_availability(deaths_loaded)
    
    # Death data path - conditional execution
    cleansed_deaths = cleanse_death_records(data_available)
    stored_deaths = store_cleansed_death_records(cleansed_deaths, deaths_table)
    cleaned_files = clean_death_temp_files(stored_deaths)
    
    # Power plant path
    plant_queries = generate_plant_sql_queries(thermal_clean, nuclear_clean)
    stored_plants = store_power_plant_records(plant_queries, plants_table)
    
    # Completion - fan-in
    # The completion op depends on both paths
    # If death data is not available, the death path ops are no-ops but still execute
    complete_staging(cleaned_files, stored_plants)

# Launch pattern
if __name__ == '__main__':
    result = french_government_etl_job.execute_in_process(
        run_config={
            "ops": {
                "fetch_nuclear_metadata": {"config": {"nuclear_metadata_url": "https://data.gouv.fr/api/nuclear"}},
                "fetch_thermal_metadata": {"config": {"thermal_metadata_url": "https://data.gouv.fr/api/thermal"}},
                "fetch_death_records_list": {"config": {"death_records_url": "https://data.gouv.fr/api/deaths"}},
                "fetch_city_coordinates": {"config": {"city_coordinates_url": "https://data.gouv.fr/api/cities"}},
                "create_deaths_table": {"config": {"postgres_connection": "postgresql://user:pass@localhost:5432/etl_db"}},
                "create_power_plants_table": {"config": {"postgres_connection": "postgresql://user:pass@localhost:5432/etl_db"}},
                "load_deaths_to_redis": {"config": {"redis_host": "localhost", "redis_port": 6379}},
                "store_cleansed_death_records": {"config": {"postgres_connection": "postgresql://user:pass@localhost:5432/etl_db"}},
                "store_power_plant_records": {"config": {"postgres_connection": "postgresql://user:pass@localhost:5432/etl_db"}},
            }
        }
    )
    if result.success:
        logger.info("Pipeline execution completed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step {event.step_key} failed: {event.event_specific_data.error}")