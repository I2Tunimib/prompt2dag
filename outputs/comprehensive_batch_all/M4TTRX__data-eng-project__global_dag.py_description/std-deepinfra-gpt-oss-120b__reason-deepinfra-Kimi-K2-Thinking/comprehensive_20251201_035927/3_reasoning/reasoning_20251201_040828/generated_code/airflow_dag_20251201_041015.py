from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
import requests
import pandas as pd
import os
import json

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}


def fetch_nuclear_metadata(**context):
    """Fetch nuclear power plant metadata from French government API."""
    metadata = {
        'url': 'https://example.com/nuclear_plants.csv',
        'format': 'csv',
        'last_updated': datetime.now().isoformat()
    }
    context['task_instance'].xcom_push(key='nuclear_metadata', value=metadata)
    return metadata


def fetch_thermal_metadata(**context):
    """Fetch thermal power plant metadata from French government API."""
    metadata = {
        'url': 'https://example.com/thermal_plants.csv',
        'format': 'csv',
        'last_updated': datetime.now().isoformat()
    }
    context['task_instance'].xcom_push(key='thermal_metadata', value=metadata)
    return metadata


def fetch_death_records_resource_list(**context):
    """Fetch death records resource list from national registry."""
    resources = [
        {'url': 'https://example.com/deaths_2023.csv', 'year': 2023},
        {'url': 'https://example.com/deaths_2022.csv', 'year': 2022},
    ]
    context['task_instance'].xcom_push(key='death_resources', value=resources)
    return resources


def fetch_city_coordinates(**context):
    """Fetch city geographic coordinates CSV."""
    coordinates_url = 'https://example.com/city_coordinates.csv'
    context['task_instance'].xcom_push(key='coordinates_url', value=coordinates_url)
    return coordinates_url


def download_nuclear_data(**context):
    """Download nuclear power plant CSV data based on metadata."""
    ti = context['task_instance']
    metadata = ti.xcom_pull(task_ids='fetch_nuclear_metadata', key='nuclear_metadata')
    
    data = [
        {'plant_id': 'N001', 'name': 'Paluel', 'capacity_mw': 1330, 'status': 'operational'},
        {'plant_id': 'N002', 'name': 'Flamanville', 'capacity_mw': 1330, 'status': 'operational'},
    ]
    
    output_path = '/tmp/nuclear_plants_raw.csv'
    pd.DataFrame(data).to_csv(output_path, index=False)
    ti.xcom_push(key='nuclear_data_path', value=output_path)
    return output_path


def download_thermal_data(**context):
    """Download thermal power plant CSV data based on metadata."""
    ti = context['task_instance']
    metadata = ti.xcom_pull(task_ids='fetch_thermal_metadata', key='thermal_metadata')
    
    data = [
        {'plant_id': 'T001', 'name': 'Cordemais', 'capacity_mw': 1200, 'fuel_type': 'coal', 'status': 'operational'},
        {'plant_id': 'T002', 'name': 'Le Havre', 'capacity_mw': 600, 'fuel_type': 'gas', 'status': 'operational'},
    ]
    
    output_path = '/tmp/thermal_plants_raw.csv'
    pd.DataFrame(data).to_csv(output_path, index=False)
    ti.xcom_push(key='thermal_data_path', value=output_path)
    return output_path


def download_death_record_files(**context):
    """Download death record files based on resource list."""
    ti = context['task_instance']
    resources = ti.xcom_pull(task_ids='fetch_death_records_resource_list', key='death_resources')
    
    downloaded_files = []
    for resource in resources:
        file_path = f"/tmp/deaths_{resource['year']}.csv"
        sample_data = [
            {'id': 1, 'date': '2023-01-15', 'city': 'Paris', 'age': 75},
            {'id': 2, 'date': '2023-01-16', 'city': 'Lyon', 'age': 82},
        ]
        pd.DataFrame(sample_data).to_csv(file_path, index=False)
        downloaded_files.append(file_path)
    
    ti.xcom_push(key='death_files', value=downloaded_files)
    return downloaded_files


def create_deaths_table(**context):
    """Create deaths table in PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        record_id INTEGER,
        death_date DATE,
        city VARCHAR(100),
        age_at_death INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_table_sql)


def create_power_plants_table(**context):
    """Create power_plants table in PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        plant_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(200),
        capacity_mw INTEGER,
        plant_type VARCHAR(50),
        fuel_type VARCHAR(50),
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_table_sql)


def load_death_records_to_redis(**context):
    """Load death records from ingestion files into Redis."""
    ti = context['task_instance']
    death_files = ti.xcom_pull(task_ids='download_death_record_files', key='death_files')
    
    redis_hook = RedisHook(redis_conn_id='redis_default')
    redis_client = redis_hook.get_conn()
    
    for file_path in death_files:
        redis_client.set(f"death_file:{os.path.basename(file_path)}", "loaded")
    
    return f"Loaded {len(death_files)} death record files to Redis"


def clean_thermal_plant_data(**context):
    """Clean thermal plant data with column renaming and filtering."""
    ti = context['task_instance']
    thermal_path = ti.xcom_pull(task_ids='download_thermal_data', key='thermal_data_path')
    
    df = pd.read_csv(thermal_path)
    df = df.rename(columns={'plant_id': 'id', 'capacity_mw': 'capacity'})
    df = df[df['status'] == 'operational']
    
    cleaned_path = '/tmp/thermal_plants_cleaned.csv'
    df.to_csv(cleaned_path, index=False)
    ti.xcom_push(key='thermal_cleaned_path', value=cleaned_path)
    return cleaned_path


def clean_nuclear_plant_data(**context):
    """Clean nuclear plant data with column renaming and filtering."""
    ti = context['task_instance']
    nuclear_path = ti.xcom_pull(task_ids='download_nuclear_data', key='nuclear_data_path')
    
    df = pd.read_csv(nuclear_path)
    df = df.rename(columns={'plant_id': 'id', 'capacity_mw': 'capacity'})
    df = df[df['status'] == 'operational']
    
    cleaned_path = '/tmp/nuclear_plants_cleaned.csv'
    df.to_csv(cleaned_path, index=False)
    ti.xcom_push(key='nuclear_cleaned_path', value=cleaned_path)
    return cleaned_path


def check_death_data_availability(**context):
    """Branch based on death data availability."""
    ti = context['task_instance']
    death_files = ti.xcom_pull(task_ids='download_death_record_files', key='death_files')
    
    if death_files and len(death_files) > 0:
        return 'cleanse_death_records'
    else:
        return 'staging_completion'


def cleanse_death_records(**context):
    """Cleanse death records with date formatting and geographic enrichment."""
    ti = context['task_instance']
    death_files = ti.xcom_pull(task_ids='download_death_record_files', key='death_files')
    
    cleansed_records = []
    for file_path in death_files:
        df = pd.read_csv(file_path)
        df['death_date'] = pd.to_datetime(df['date'])
        df['city_code'] = df['city'].apply(lambda x: f"CODE_{x}")
        cleansed_records.append(df)
    
    combined_df = pd.concat(cleansed_records, ignore_index=True)
    output_path = '/tmp/deaths_cleansed.csv'
    combined_df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='cleansed_deaths_path', value=output_path)
    return output_path


def store_cleansed_death_records(**context):
    """Store cleansed death records in PostgreSQL."""
    ti = context['task_instance']
    cleansed_path = ti.xcom_pull(task_ids='cleanse_death_records', key='cleansed_deaths_path')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(cleansed_path)
    
    for _, row in df.iterrows():
        pg_hook.run(
            """
            INSERT INTO deaths (record_id, death_date, city, age_at_death)
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(row['id'], row['death_date'], row['city'], row['age'])
        )


def clean_temporary_death_files(**context):
    """Clean temporary death data files."""
    ti = context['task_instance']
    death_files = ti.xcom_pull(task_ids='download_death_record_files', key='death_files')
    
    for file_path in death_files:
        if os.path.exists(file_path):
            os.remove(file_path)
    
    cleansed_path = ti.xcom_pull(task_ids='cleanse_death_records', key='cleansed_deaths_path')
    if cleansed_path and os.path.exists(cleansed_path):
        os.remove(cleansed_path)
    
    return "Temporary death files cleaned"


def generate_plant_sql_queries(**context):
    """Generate SQL queries for plant data persistence."""
    queries = {
        'nuclear': "INSERT INTO power_plants (plant_id, name, capacity_mw, plant_type) VALUES (%s, %s, %s, 'nuclear')",
        'thermal': "INSERT INTO power_plants (plant_id, name, capacity_mw, plant_type, fuel_type) VALUES (%s, %s, %s, 'thermal', %s)"
    }
    return queries


def store_power_plant_records(**context):
    """Store power plant records in PostgreSQL."""
    ti = context['task_instance']
    
    nuclear_path = ti.xcom_pull(task_ids='clean_nuclear_plant_data', key='nuclear_cleaned_path')
    thermal_path = ti.xcom_pull(task_ids='clean_thermal_plant_data', key='thermal_cleaned_path')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    if nuclear_path and os.path.exists(nuclear_path):
        df_nuclear = pd.read_csv(nuclear_path)
        for _, row in df_nuclear.iterrows():
            pg_hook.run(
                "INSERT INTO power_plants (plant_id, name, capacity_mw, plant_type) VALUES (%s, %s, %s, 'nuclear')",
                parameters=(row['id'], row['name'], row['capacity'])
            )
    
    if thermal_path and os.path.exists(thermal_path):
        df_thermal = pd.read_csv(thermal_path)
        for _, row in df_thermal.iterrows():
            pg_hook.run(
                "INSERT INTO power_plants (plant_id, name, capacity_mw, plant_type, fuel_type) VALUES (%s, %s, %s, 'thermal', %s)",
                parameters=(row['id'], row['name'], row['capacity'], row.get('fuel_type', 'unknown'))
            )


def staging_completion(**context):
    """Mark staging phase completion."""
    print("Staging phase completed successfully")
    return "Staging completed"


with DAG(
    dag_id='french_government_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for French government death records and power plant data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'france', 'government', 'energy', 'mortality'],
) as dag:
    
    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python