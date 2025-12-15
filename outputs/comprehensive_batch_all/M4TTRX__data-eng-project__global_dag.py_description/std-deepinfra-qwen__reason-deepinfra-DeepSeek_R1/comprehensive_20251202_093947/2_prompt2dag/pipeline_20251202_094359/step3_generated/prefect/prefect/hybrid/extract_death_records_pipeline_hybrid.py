from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Completed
from prefect.task_runners import ConcurrentTaskRunner

# Task definitions
@task(name='cleanse_power_plant_data', retries=1)
def cleanse_power_plant_data():
    """Task: Cleanse Power Plant Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='generate_plant_insert_queries', retries=1)
def generate_plant_insert_queries():
    """Task: Generate Plant Insert Queries"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_nuclear_plants', retries=1)
def extract_nuclear_plants():
    """Task: Extract Nuclear Plants"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='create_power_plant_table', retries=1)
def create_power_plant_table():
    """Task: Create Power Plant Table"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_city_geo_data', retries=1)
def extract_city_geo_data():
    """Task: Extract City Geo Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_thermal_plants', retries=1)
def extract_thermal_plants():
    """Task: Extract Thermal Plants"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='store_plants_in_postgres', retries=1)
def store_plants_in_postgres():
    """Task: Store Plants in PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='clean_tmp_death_files', retries=1)
def clean_tmp_death_files():
    """Task: Clean Temporary Death Files"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='check_death_data_emptiness', retries=1)
def check_death_data_emptiness():
    """Task: Check Death Data Emptiness"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='store_deaths_in_postgres', retries=1)
def store_deaths_in_postgres():
    """Task: Store Deaths in PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='create_death_table', retries=1)
def create_death_table():
    """Task: Create Death Table"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_death_records', retries=1)
def extract_death_records():
    """Task: Extract Death Records"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_death_records_to_redis', retries=1)
def load_death_records_to_redis():
    """Task: Load Death Records to Redis"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanse_death_data', retries=1)
def cleanse_death_data():
    """Task: Cleanse Death Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

# Flow definition
@flow(name="extract_death_records_pipeline", task_runner=ConcurrentTaskRunner())
def extract_death_records_pipeline():
    logger = get_run_logger()

    # Entry points
    cleanse_power_plant_data()
    generate_plant_insert_queries()

    # Ingestion pipeline start
    ingestion_pipeline_start = extract_death_records.submit()
    extract_nuclear_plants.submit(wait_for=[ingestion_pipeline_start])
    extract_thermal_plants.submit(wait_for=[ingestion_pipeline_start])
    extract_city_geo_data.submit(wait_for=[ingestion_pipeline_start])

    # Staging pipeline start
    staging_pipeline_start = create_death_table.submit()
    create_power_plant_table.submit(wait_for=[staging_pipeline_start])
    load_death_records_to_redis.submit(wait_for=[staging_pipeline_start])

    # Fanout pattern
    cleanse_death_data_task = cleanse_death_data.submit(wait_for=[load_death_records_to_redis])
    check_death_data_emptiness_task = check_death_data_emptiness.submit(wait_for=[cleanse_death_data_task])
    store_deaths_in_postgres.submit(wait_for=[check_death_data_emptiness_task])

    # Final tasks
    store_plants_in_postgres.submit(wait_for=[create_power_plant_table])
    clean_tmp_death_files.submit(wait_for=[store_plants_in_postgres])

if __name__ == "__main__":
    extract_death_records_pipeline()