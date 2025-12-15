from dagster import op, job, RetryPolicy, resource, Field, String, In, Out

# Resources
@resource(config_schema={"host": Field(String), "port": Field(int)})
def postgres_resource(context):
    return {
        "host": context.resource_config["host"],
        "port": context.resource_config["port"],
    }

@resource(config_schema={"host": Field(String), "port": Field(int)})
def redis_resource(context):
    return {
        "host": context.resource_config["host"],
        "port": context.resource_config["port"],
    }

# Ops
@op
def start_pipeline():
    """Start the pipeline execution."""
    pass

@op
def fetch_nuclear_power_plant_metadata():
    """Fetch nuclear power plant metadata JSON."""
    return {"data": "nuclear_metadata"}

@op
def fetch_thermal_power_plant_metadata():
    """Fetch thermal power plant metadata JSON."""
    return {"data": "thermal_metadata"}

@op
def fetch_death_records_resource_list():
    """Fetch death records resource list."""
    return {"data": "death_records_list"}

@op
def fetch_city_geographic_coordinates():
    """Fetch city geographic coordinates CSV."""
    return {"data": "city_coordinates"}

@op
def download_nuclear_power_plant_csv(metadata):
    """Download nuclear power plant CSV data."""
    return {"data": "nuclear_csv"}

@op
def download_thermal_power_plant_csv(metadata):
    """Download thermal power plant CSV data."""
    return {"data": "thermal_csv"}

@op
def download_death_record_files(resource_list):
    """Download death record files."""
    return {"data": "death_files"}

@op
def create_deaths_table(postgres):
    """Create deaths table in PostgreSQL."""
    pass

@op
def create_power_plants_table(postgres):
    """Create power_plants table in PostgreSQL."""
    pass

@op
def load_death_records_to_redis(death_files, redis):
    """Load death records from ingestion files into Redis."""
    return {"data": "death_records_in_redis"}

@op
def clean_thermal_plant_data(thermal_csv):
    """Clean thermal plant data (column renaming, filtering)."""
    return {"data": "cleaned_thermal_csv"}

@op
def clean_nuclear_plant_data(nuclear_csv):
    """Clean nuclear plant data (column renaming, filtering)."""
    return {"data": "cleaned_nuclear_csv"}

@op
def check_death_data_availability(death_records_in_redis):
    """Check if death data exists."""
    return bool(death_records_in_redis)

@op
def cleanse_death_records(death_records_in_redis):
    """Cleanse death records (date formatting, geographic enrichment)."""
    return {"data": "cleaned_death_records"}

@op
def store_cleansed_death_records(cleaned_death_records, postgres):
    """Store cleansed death records in PostgreSQL."""
    pass

@op
def clean_temporary_death_data_files(death_files):
    """Clean temporary death data files."""
    pass

@op
def generate_sql_queries_for_plant_data(cleaned_thermal_csv, cleaned_nuclear_csv):
    """Generate SQL queries for plant data persistence."""
    return {"data": "sql_queries"}

@op
def store_power_plant_records(sql_queries, postgres):
    """Store power plant records in PostgreSQL."""
    pass

@op
def complete_staging():
    """Complete staging phase."""
    pass

# Job
@job(
    resource_defs={
        "postgres": postgres_resource,
        "redis": redis_resource,
    },
    tags={"max_concurrency": 1},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
)
def etl_pipeline():
    start = start_pipeline()

    # Parallel data ingestion
    nuclear_metadata = fetch_nuclear_power_plant_metadata(start)
    thermal_metadata = fetch_thermal_power_plant_metadata(start)
    death_records_list = fetch_death_records_resource_list(start)
    city_coordinates = fetch_city_geographic_coordinates(start)

    # Process fetched metadata
    nuclear_csv = download_nuclear_power_plant_csv(nuclear_metadata)
    thermal_csv = download_thermal_power_plant_csv(thermal_metadata)
    death_files = download_death_record_files(death_records_list)

    # Create PostgreSQL tables
    create_deaths_table(start)
    create_power_plants_table(start)

    # Parallel data processing
    death_records_in_redis = load_death_records_to_redis(death_files, redis)
    cleaned_thermal_csv = clean_thermal_plant_data(thermal_csv)
    cleaned_nuclear_csv = clean_nuclear_plant_data(nuclear_csv)

    # Branch based on death data availability
    death_data_available = check_death_data_availability(death_records_in_redis)

    # Death data processing path
    cleaned_death_records = cleanse_death_records(death_records_in_redis).with_hooks(
        {death_data_available}
    )
    store_cleansed_death_records(cleaned_death_records, postgres).with_hooks(
        {death_data_available}
    )
    clean_temporary_death_data_files(death_files).with_hooks(
        {death_data_available}
    )

    # Power plant data processing
    sql_queries = generate_sql_queries_for_plant_data(cleaned_thermal_csv, cleaned_nuclear_csv)
    store_power_plant_records(sql_queries, postgres)

    # Complete staging phase
    complete_staging()

if __name__ == "__main__":
    result = etl_pipeline.execute_in_process()