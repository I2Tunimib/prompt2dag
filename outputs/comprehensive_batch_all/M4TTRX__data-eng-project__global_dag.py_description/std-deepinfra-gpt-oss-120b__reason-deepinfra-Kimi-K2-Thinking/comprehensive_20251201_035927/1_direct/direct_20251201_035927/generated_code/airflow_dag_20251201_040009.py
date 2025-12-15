from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook


def fetch_nuclear_metadata(**context):
    """Fetch nuclear power plant metadata JSON."""
    # Placeholder for API call
    metadata = {"url": "https://example.com/nuclear_metadata.json"}
    context["ti"].xcom_push(key="nuclear_metadata", value=metadata)


def fetch_thermal_metadata(**context):
    """Fetch thermal power plant metadata JSON."""
    metadata = {"url": "https://example.com/thermal_metadata.json"}
    context["ti"].xcom_push(key="thermal_metadata", value=metadata)


def fetch_death_resource_list(**context):
    """Fetch death records resource list."""
    resource_list = {"url": "https://example.com/death_records.json"}
    context["ti"].xcom_push(key="death_resource_list", value=resource_list)


def fetch_city_geo_csv(**context):
    """Fetch city geographic coordinates CSV."""
    csv_info = {"url": "https://example.com/cities.csv"}
    context["ti"].xcom_push(key="city_geo_csv", value=csv_info)


def download_nuclear_csv(**context):
    """Download nuclear power plant CSV data based on fetched metadata."""
    metadata = context["ti"].xcom_pull(key="nuclear_metadata", task_ids="ingestion.fetch_nuclear_metadata")
    # Placeholder download logic
    csv_path = "/tmp/nuclear_plants.csv"
    context["ti"].xcom_push(key="nuclear_csv_path", value=csv_path)


def download_thermal_csv(**context):
    """Download thermal power plant CSV data based on fetched metadata."""
    metadata = context["ti"].xcom_pull(key="thermal_metadata", task_ids="ingestion.fetch_thermal_metadata")
    csv_path = "/tmp/thermal_plants.csv"
    context["ti"].xcom_push(key="thermal_csv_path", value=csv_path)


def download_death_files(**context):
    """Download death record files based on resource list."""
    resource = context["ti"].xcom_pull(key="death_resource_list", task_ids="ingestion.fetch_death_resource_list")
    file_path = "/tmp/death_records.json"
    context["ti"].xcom_push(key="death_file_path", value=file_path)


def create_deaths_table(**context):
    """Create deaths table in PostgreSQL."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    create_sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city VARCHAR,
        insee_code VARCHAR,
        other_details JSONB
    );
    """
    pg.run(create_sql)


def create_power_plants_table(**context):
    """Create power_plants table in PostgreSQL."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    create_sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_type VARCHAR,
        name VARCHAR,
        capacity_mw NUMERIC,
        location VARCHAR,
        other_details JSONB
    );
    """
    pg.run(create_sql)


def load_death_to_redis(**context):
    """Load death records into Redis for temporary storage."""
    redis = RedisHook(redis_conn_id="redis_default")
    # Placeholder: simulate loading and set existence flag
    death_exists = True  # In real implementation, check file size or content
    context["ti"].xcom_push(key="death_data_exists", value=death_exists)


def clean_thermal_data(**context):
    """Clean thermal plant data (renaming columns, filtering)."""
    # Placeholder for data cleaning logic
    pass


def clean_nuclear_data(**context):
    """Clean nuclear plant data (renaming columns, filtering)."""
    # Placeholder for data cleaning logic
    pass


def branch_on_death_data(**context):
    """Branch depending on whether death data is available."""
    death_exists = context["ti"].xcom_pull(key="death_data_exists", task_ids="processing.load_death_to_redis")
    if death_exists:
        return "death_processing.cleanse_death_records"
    return "skip_death"


def cleanse_death_records(**context):
    """Cleanse death records (date formatting, geographic enrichment)."""
    # Placeholder for cleansing logic
    pass


def store_death_to_postgres(**context):
    """Store cleansed death records into PostgreSQL."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    # Placeholder insert statement
    insert_sql = "INSERT INTO deaths (death_date, city, insee_code, other_details) VALUES (%s, %s, %s, %s);"
    # In real implementation, iterate over cleaned records
    pg.run(insert_sql, parameters=("2023-01-01", "Paris", "75056", "{}"))


def clean_temp_death_files(**context):
    """Remove temporary death data files."""
    # Placeholder for file cleanup
    pass


def generate_plant_sql(**context):
    """Generate SQL queries for power plant data persistence."""
    # Placeholder for SQL generation
    context["ti"].xcom_push(key="plant_insert_sql", value="INSERT INTO power_plants (plant_type, name, capacity_mw, location, other_details) VALUES (%s, %s, %s, %s, %s);")


def store_plant_to_postgres(**context):
    """Store power plant records into PostgreSQL."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    insert_sql = context["ti"].xcom_pull(key="plant_insert_sql", task_ids="power_plant_processing.generate_plant_sql")
    # Placeholder execution
    pg.run(insert_sql, parameters=("Nuclear", "Plant A", 1200, "Location X", "{}"))


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "start_date": days_ago(1),
}


with DAG(
    dag_id="french_government_etl",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["etl", "government", "power_plants", "deaths"],
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="ingestion") as ingestion:
        fetch_nuclear = PythonOperator(
            task_id="fetch_nuclear_metadata",
            python_callable=fetch_nuclear_metadata,
        )
        fetch_thermal = PythonOperator(
            task_id="fetch_thermal_metadata",
            python_callable=fetch_thermal_metadata,
        )
        fetch_death = PythonOperator(
            task_id="fetch_death_resource_list",
            python_callable=fetch_death_resource_list,
        )
        fetch_city = PythonOperator(
            task_id="fetch_city_geo_csv",
            python_callable=fetch_city_geo_csv,
        )
        fetch_nuclear >> fetch_thermal >> fetch_death >> fetch_city

    with TaskGroup(group_id="download") as download:
        download_nuclear = PythonOperator(
            task_id="download_nuclear_csv",
            python_callable=download_nuclear_csv,
        )
        download_thermal = PythonOperator(
            task_id="download_thermal_csv",
            python_callable=download_thermal_csv,
        )
        download_death = PythonOperator(
            task_id="download_death_files",
            python_callable=download_death_files,
        )
        download_nuclear >> download_thermal >> download_death

    with TaskGroup(group_id="create_tables") as create_tables:
        create_deaths = PythonOperator(
            task_id="create_deaths_table",
            python_callable=create_deaths_table,
        )
        create_plants = PythonOperator(
            task_id="create_power_plants_table",
            python_callable=create_power_plants_table,
        )
        create_deaths >> create_plants

    with TaskGroup(group_id="processing") as processing:
        load_death = PythonOperator(
            task_id="load_death_to_redis",
            python_callable=load_death_to_redis,
        )
        clean_thermal = PythonOperator(
            task_id="clean_thermal_data",
            python_callable=clean_thermal_data,
        )
        clean_nuclear = PythonOperator(
            task_id="clean_nuclear_data",
            python_callable=clean_nuclear_data,
        )
        load_death >> clean_thermal >> clean_nuclear

    branch = BranchPythonOperator(
        task_id="branch_on_death_data",
        python_callable=branch_on_death_data,
    )

    skip_death = DummyOperator(task_id="skip_death")

    with TaskGroup(group_id="death_processing") as death_processing:
        cleanse_death = PythonOperator(
            task_id="cleanse_death_records",
            python_callable=cleanse_death_records,
        )
        store_death = PythonOperator(
            task_id="store_death_to_postgres",
            python_callable=store_death_to_postgres,
        )
        clean_temp = PythonOperator(
            task_id="clean_temp_death_files",
            python_callable=clean_temp_death_files,
        )
        cleanse_death >> store_death >> clean_temp

    with TaskGroup(group_id="power_plant_processing") as power_plant_processing:
        generate_sql = PythonOperator(
            task_id="generate_plant_sql",
            python_callable=generate_plant_sql,
        )
        store_plant = PythonOperator(
            task_id="store_plant_to_postgres",
            python_callable=store_plant_to_postgres,
        )
        generate_sql >> store_plant

    staging_complete = DummyOperator(task_id="staging_complete")

    # Define dependencies
    start >> ingestion >> download >> create_tables >> processing >> branch
    branch >> [death_processing, skip_death]
    death_processing >> power_plant_processing
    skip_death >> power_plant_processing
    power_plant_processing >> staging_complete