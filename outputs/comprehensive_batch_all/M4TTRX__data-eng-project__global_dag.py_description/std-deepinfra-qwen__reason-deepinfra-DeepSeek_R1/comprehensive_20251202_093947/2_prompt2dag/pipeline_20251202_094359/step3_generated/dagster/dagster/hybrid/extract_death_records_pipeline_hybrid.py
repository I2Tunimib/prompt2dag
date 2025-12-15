from dagster import job, op, graph, In, Out, ResourceDefinition, fs_io_manager, multiprocess_executor

# Task Definitions
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
    name='generate_plant_insert_queries',
    description='Generate Plant Insert Queries',
)
def generate_plant_insert_queries(context):
    """Op: Generate Plant Insert Queries"""
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
    name='store_deaths_in_postgres',
    description='Store Deaths in PostgreSQL',
)
def store_deaths_in_postgres(context):
    """Op: Store Deaths in PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_city_geo_data',
    description='Extract City Geo Data',
)
def extract_city_geo_data(context):
    """Op: Extract City Geo Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_nuclear_plants',
    description='Extract Nuclear Plants',
)
def extract_nuclear_plants(context):
    """Op: Extract Nuclear Plants"""
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
    name='clean_tmp_death_files',
    description='Clean Temporary Death Files',
)
def clean_tmp_death_files(context):
    """Op: Clean Temporary Death Files"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_thermal_plants',
    description='Extract Thermal Plants',
)
def extract_thermal_plants(context):
    """Op: Extract Thermal Plants"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='create_power_plant_table',
    description='Create Power Plant Table',
)
def create_power_plant_table(context):
    """Op: Create Power Plant Table"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_death_records',
    description='Extract Death Records',
)
def extract_death_records(context):
    """Op: Extract Death Records"""
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

# Job Definition
@job(
    name='extract_death_records_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'io_manager': fs_io_manager,
        'redis': ResourceDefinition.mock_resource(),
        'data_gouv_api': ResourceDefinition.mock_resource(),
        'postgres_default': ResourceDefinition.mock_resource(),
    },
)
def extract_death_records_pipeline():
    # Ingestion Pipeline Start
    ingestion_pipeline_start = extract_death_records()
    ingestion_pipeline_start = extract_nuclear_plants()
    ingestion_pipeline_start = extract_thermal_plants()
    ingestion_pipeline_start = extract_city_geo_data()

    # Staging Pipeline Start
    staging_pipeline_start = create_death_table()
    staging_pipeline_start = create_power_plant_table()
    staging_pipeline_start = load_death_records_to_redis()

    # Cleanse and Check Death Data
    cleanse_death_data_result = cleanse_death_data(load_death_records_to_redis)
    check_death_data_emptiness_result = check_death_data_emptiness(cleanse_death_data_result)

    # Store Deaths in PostgreSQL
    store_deaths_in_postgres_result = store_deaths_in_postgres(check_death_data_emptiness_result)

    # Cleanse Power Plant Data
    cleanse_power_plant_data_result = cleanse_power_plant_data()
    generate_plant_insert_queries_result = generate_plant_insert_queries()

    # Store Plants in PostgreSQL
    store_plants_in_postgres_result = store_plants_in_postgres()

    # Clean Temporary Death Files
    clean_tmp_death_files_result = clean_tmp_death_files(store_plants_in_postgres_result)
```
This code defines the `extract_death_records_pipeline` job with the specified task dependencies and the 'fanout' pattern. The job uses the `multiprocess_executor` and the required resources are defined. The task definitions are included exactly as provided.