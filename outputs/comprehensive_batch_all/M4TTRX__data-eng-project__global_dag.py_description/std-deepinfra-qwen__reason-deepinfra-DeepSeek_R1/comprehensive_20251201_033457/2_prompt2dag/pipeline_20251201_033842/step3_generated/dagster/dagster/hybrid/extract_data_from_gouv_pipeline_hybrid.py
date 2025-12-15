from dagster import job, op, graph, In, Out, ResourceDefinition, fs_io_manager, multiprocess_executor

# Task Definitions
@op(
    name='cleanse_death_data',
    description='Cleanse Death Data',
)
def cleanse_death_data(context):
    """Op: Cleanse Death Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='check_death_data_emptiness',
    description='Check Death Data Emptiness',
)
def check_death_data_emptiness(context):
    """Op: Check Death Data Emptiness"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='staging_end',
    description='Staging End',
)
def staging_end(context):
    """Op: Staging End"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='store_deaths_in_postgres',
    description='Store Deaths in PostgreSQL',
)
def store_deaths_in_postgres(context):
    """Op: Store Deaths in PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_thermal_data',
    description='Fetch Thermal Power Plant Data',
)
def fetch_thermal_data(context):
    """Op: Fetch Thermal Power Plant Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_nuclear_data',
    description='Fetch Nuclear Power Plant Data',
)
def fetch_nuclear_data(context):
    """Op: Fetch Nuclear Power Plant Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='clean_tmp_death_files',
    description='Clean Temporary Death Files',
)
def clean_tmp_death_files(context):
    """Op: Clean Temporary Death Files"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='create_death_table',
    description='Create Death Table',
)
def create_death_table(context):
    """Op: Create Death Table"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='cleanse_power_plant_data',
    description='Cleanse Power Plant Data',
)
def cleanse_power_plant_data(context):
    """Op: Cleanse Power Plant Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='generate_plant_persist_sql',
    description='Generate Plant Persist SQL',
)
def generate_plant_persist_sql(context):
    """Op: Generate Plant Persist SQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='store_plants_in_postgres',
    description='Store Plants in PostgreSQL',
)
def store_plants_in_postgres(context):
    """Op: Store Plants in PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_death_records',
    description='Fetch Death Records',
)
def fetch_death_records(context):
    """Op: Fetch Death Records"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_data_from_gouv',
    description='Extract Data from data.gouv.fr',
)
def extract_data_from_gouv(context):
    """Op: Extract Data from data.gouv.fr"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_death_records_to_redis',
    description='Load Death Records to Redis',
)
def load_death_records_to_redis(context):
    """Op: Load Death Records to Redis"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_city_geo',
    description='Download City Geographic Coordinates',
)
def download_city_geo(context):
    """Op: Download City Geographic Coordinates"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='create_power_plants_table',
    description='Create Power Plants Table',
)
def create_power_plants_table(context):
    """Op: Create Power Plants Table"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="extract_data_from_gouv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "postgres_default": ResourceDefinition.hardcoded_resource(None, "postgres_default"),
        "redis": ResourceDefinition.hardcoded_resource(None, "redis"),
        "data_gouv_api": ResourceDefinition.hardcoded_resource(None, "data_gouv_api"),
    },
)
def extract_data_from_gouv_pipeline():
    ingestion_pipeline_start = extract_data_from_gouv()
    download_city_geo(ingestion_pipeline_start)
    fetch_nuclear_data(ingestion_pipeline_start)
    fetch_thermal_data(ingestion_pipeline_start)
    fetch_death_records(ingestion_pipeline_start)

    staging_pipeline_start = create_death_table()
    create_power_plants_table(staging_pipeline_start)
    load_death_records_to_redis(staging_pipeline_start)

    cleanse_death_data(create_death_table, load_death_records_to_redis)
    cleanse_power_plant_data(create_power_plants_table)

    generate_plant_persist_sql(cleanse_power_plant_data)

    check_death_data_emptiness(cleanse_death_data)
    store_deaths_in_postgres(check_death_data_emptiness)
    staging_end(check_death_data_emptiness)

    store_plants_in_postgres(generate_plant_persist_sql, staging_end)

    clean_tmp_death_files(store_deaths_in_postgres, store_plants_in_postgres)