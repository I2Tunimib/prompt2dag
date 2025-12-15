from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

@task(name='ingestion_pipeline_start', retries=1)
def ingestion_pipeline_start():
    """Task: Ingestion Pipeline Start"""
    pass

@task(name='staging_pipeline_start', retries=1)
def staging_pipeline_start():
    """Task: Staging Pipeline Start"""
    pass

@task(name='fetch_thermal_data', retries=1)
def fetch_thermal_data():
    """Task: Fetch Thermal Power Plant Data"""
    pass

@task(name='store_plants_in_postgres', retries=1)
def store_plants_in_postgres():
    """Task: Store Plants in PostgreSQL"""
    pass

@task(name='extract_data_from_gouv', retries=1)
def extract_data_from_gouv():
    """Task: Extract Data from data.gouv.fr"""
    pass

@task(name='fetch_death_records', retries=1)
def fetch_death_records():
    """Task: Fetch Death Records"""
    pass

@task(name='load_death_records_to_redis', retries=1)
def load_death_records_to_redis():
    """Task: Load Death Records to Redis"""
    pass

@task(name='download_city_geo', retries=1)
def download_city_geo():
    """Task: Download City Geographic Coordinates"""
    pass

@task(name='create_death_table', retries=1)
def create_death_table():
    """Task: Create Death Table"""
    pass

@task(name='cleanse_death_data', retries=1)
def cleanse_death_data():
    """Task: Cleanse Death Data"""
    pass

@task(name='check_death_data_emptiness', retries=1)
def check_death_data_emptiness():
    """Task: Check Death Data Emptiness"""
    pass

@task(name='staging_end', retries=1)
def staging_end():
    """Task: Staging End"""
    pass

@task(name='store_deaths_in_postgres', retries=1)
def store_deaths_in_postgres():
    """Task: Store Deaths in PostgreSQL"""
    pass

@task(name='clean_tmp_death_files', retries=1)
def clean_tmp_death_files():
    """Task: Clean Temporary Death Files"""
    pass

@task(name='cleanse_power_plant_data', retries=1)
def cleanse_power_plant_data():
    """Task: Cleanse Power Plant Data"""
    pass

@task(name='generate_plant_persist_sql', retries=1)
def generate_plant_persist_sql():
    """Task: Generate Plant Persist SQL"""
    pass

@task(name='create_power_plants_table', retries=1)
def create_power_plants_table():
    """Task: Create Power Plants Table"""
    pass

@task(name='fetch_nuclear_data', retries=1)
def fetch_nuclear_data():
    """Task: Fetch Nuclear Power Plant Data"""
    pass

@flow(name="extract_data_from_gouv_pipeline", task_runner=ConcurrentTaskRunner())
def extract_data_from_gouv_pipeline():
    logger = get_run_logger()
    logger.info("Starting the extract_data_from_gouv_pipeline")

    ingestion_start = ingestion_pipeline_start()
    
    extract_data = extract_data_from_gouv.submit(wait_for=[ingestion_start])
    download_geo = download_city_geo.submit(wait_for=[ingestion_start])
    fetch_nuclear = fetch_nuclear_data.submit(wait_for=[ingestion_start])
    fetch_thermal = fetch_thermal_data.submit(wait_for=[ingestion_start])
    fetch_death = fetch_death_records.submit(wait_for=[ingestion_start])

    staging_start = staging_pipeline_start()

    create_death_table_task = create_death_table.submit(wait_for=[staging_start])
    load_death_records_task = load_death_records_to_redis.submit(wait_for=[staging_start])
    create_power_plants_table_task = create_power_plants_table.submit(wait_for=[staging_start])

    cleanse_death_data_task = cleanse_death_data.submit(wait_for=[create_death_table_task, load_death_records_task])
    cleanse_power_plant_data_task = cleanse_power_plant_data.submit(wait_for=[create_power_plants_table_task])

    generate_plant_persist_sql_task = generate_plant_persist_sql.submit(wait_for=[cleanse_power_plant_data_task])
    check_death_data_emptiness_task = check_death_data_emptiness.submit(wait_for=[cleanse_death_data_task])

    store_deaths_task = store_deaths_in_postgres.submit(wait_for=[check_death_data_emptiness_task])
    staging_end_task = staging_end.submit(wait_for=[check_death_data_emptiness_task])

    store_plants_task = store_plants_in_postgres.submit(wait_for=[generate_plant_persist_sql_task, staging_end_task])

    clean_tmp_death_files.submit(wait_for=[store_deaths_task, store_plants_task])

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    extract_data_from_gouv_pipeline()