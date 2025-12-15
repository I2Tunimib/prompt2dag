import os
from datetime import datetime, timedelta

import pandas as pd
import requests
import redis
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}


def fetch_nuclear_metadata(**context):
    """Fetch nuclear power plant metadata JSON."""
    url = "https://example.com/api/nuclear_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_thermal_metadata(**context):
    """Fetch thermal power plant metadata JSON."""
    url = "https://example.com/api/thermal_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_death_records_list(**context):
    """Fetch death records resource list JSON."""
    url = "https://example.com/api/death_records_list.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_city_coordinates(**context):
    """Fetch city geographic coordinates CSV."""
    url = "https://example.com/api/cities_coordinates.csv"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = "/tmp/cities_coordinates.csv"
    with open(path, "wb") as f:
        f.write(response.content)
    return path


def download_nuclear_csv(**context):
    """Download nuclear power plant CSV using metadata."""
    metadata = context["ti"].xcom_pull(task_ids="ingestion.fetch_nuclear_metadata")
    csv_url = metadata.get("csv_url", "https://example.com/data/nuclear.csv")
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    path = "/tmp/nuclear.csv"
    with open(path, "wb") as f:
        f.write(response.content)
    return path


def download_thermal_csv(**context):
    """Download thermal power plant CSV using metadata."""
    metadata = context["ti"].xcom_pull(task_ids="ingestion.fetch_thermal_metadata")
    csv_url = metadata.get("csv_url", "https://example.com/data/thermal.csv")
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    path = "/tmp/thermal.csv"
    with open(path, "wb") as f:
        f.write(response.content)
    return path


def download_death_records(**context):
    """Download death records CSV using resource list."""
    resource_list = context["ti"].xcom_pull(task_ids="ingestion.fetch_death_records_list")
    csv_url = resource_list.get("csv_url", "https://example.com/data/deaths.csv")
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    path = "/tmp/deaths_raw.csv"
    with open(path, "wb") as f:
        f.write(response.content)
    return path


def create_deaths_table(**context):
    """Create deaths table in PostgreSQL."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR(10),
        age INT,
        gender VARCHAR(10)
    );
    """
    hook.run(sql)


def create_power_plants_table(**context):
    """Create power_plants table in PostgreSQL."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_name VARCHAR(255),
        plant_type VARCHAR(50),
        capacity_mw NUMERIC,
        city_code VARCHAR(10)
    );
    """
    hook.run(sql)


def load_death_records_to_redis(**context):
    """Load raw death records into Redis for intermediate storage."""
    path = context["ti"].xcom_pull(task_ids="ingestion.download_death_records")
    if not path or not os.path.exists(path):
        return
    r = redis.Redis(host="localhost", port=6379, db=0)
    df = pd.read_csv(path)
    for _, row in df.iterrows():
        key = f"death:{row['id']}"
        r.hmset(key, row.to_dict())


def clean_thermal_data(**context):
    """Clean thermal plant data: rename columns and filter active plants."""
    path = context["ti"].xcom_pull(task_ids="ingestion.download_thermal_csv")
    df = pd.read_csv(path)
    df = df.rename(columns={"PlantName": "plant_name", "CapacityMW": "capacity_mw", "Status": "status"})
    df = df[df["status"] == "active"]
    cleaned_path = "/tmp/thermal_cleaned.csv"
    df.to_csv(cleaned_path, index=False)
    return cleaned_path


def clean_nuclear_data(**context):
    """Clean nuclear plant data: rename columns and filter operational plants."""
    path = context["ti"].xcom_pull(task_ids="ingestion.download_nuclear_csv")
    df = pd.read_csv(path)
    df = df.rename(columns={"Name": "plant_name", "MW": "capacity_mw", "Operational": "operational"})
    df = df[df["operational"] == True]
    cleaned_path = "/tmp/nuclear_cleaned.csv"
    df.to_csv(cleaned_path, index=False)
    return cleaned_path


def branch_on_death_data(**context):
    """Branch depending on whether death data file exists and is non‑empty."""
    path = context["ti"].xcom_pull(task_ids="ingestion.download_death_records")
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        return "death_processing.cleanse_death_records"
    return "skip_death_processing"


def cleanse_death_records(**context):
    """Cleanse death records: format dates and enrich with city coordinates."""
    deaths_path = context["ti"].xcom_pull(task_ids="ingestion.download_death_records")
    cities_path = context["ti"].xcom_pull(task_ids="ingestion.fetch_city_coordinates")
    deaths_df = pd.read_csv(deaths_path)
    deaths_df["death_date"] = pd.to_datetime(deaths_df["death_date"], errors="coerce").dt.date
    cities_df = pd.read_csv(cities_path)
    enriched_df = deaths_df.merge(cities_df, how="left", left_on="city_code", right_on="city_code")
    cleaned_path = "/tmp/deaths_cleaned.csv"
    enriched_df.to_csv(cleaned_path, index=False)
    return cleaned_path


def store_death_records_pg(**context):
    """Store cleansed death records into PostgreSQL."""
    path = context["ti"].xcom_pull(task_ids="death_processing.cleanse_death_records")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = f"COPY deaths (death_date, city_code, age, gender) FROM STDIN WITH CSV HEADER DELIMITER ','"
    with open(path, "r") as f:
        hook.copy_expert(sql, f)


def clean_temp_death_files(**context):
    """Remove temporary death data files."""
    raw_path = context["ti"].xcom_pull(task_ids="ingestion.download_death_records")
    cleaned_path = context["ti"].xcom_pull(task_ids="death_processing.cleanse_death_records")
    for p in [raw_path, cleaned_path]:
        if p and os.path.exists(p):
            os.remove(p)


def generate_power_plant_sql(**context):
    """Generate SQL statements for inserting power plant records."""
    thermal_path = context["ti"].xcom_pull(task_ids="processing.clean_thermal_data")
    nuclear_path = context["ti"].xcom_pull(task_ids="processing.clean_nuclear_data")
    df_thermal = pd.read_csv(thermal_path)
    df_nuclear = pd.read_csv(nuclear_path)
    combined = pd.concat([df_thermal, df_nuclear], ignore_index=True)
    combined["plant_type"] = combined.apply(
        lambda row: "nuclear" if "nuclear" in row["plant_name"].lower() else "thermal", axis=1
    )
    sql_path = "/tmp/power_plants_insert.sql"
    with open(sql_path, "w") as f:
        for _, row in combined.iterrows():
            values = (
                f"'{row['plant_name']}', "
                f"'{row['plant_type']}', "
                f"{row['capacity_mw']}, "
                f"'{row.get('city_code', '')}'"
            )
            f.write(f"INSERT INTO power_plants (plant_name, plant_type, capacity_mw, city_code) VALUES ({values});\n")
    return sql_path


def store_power_plant_records_pg(**context):
    """Execute generated SQL to store power plant records."""
    sql_path = context["ti"].xcom_pull(task_ids="power_processing.generate_power_plant_sql")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with open(sql_path, "r") as f:
        sql_statements = f.read()
    hook.run(sql_statements)


with DAG(
    dag_id="french_government_etl",
    default_args=default_args,
    description="ETL pipeline for French government death records and power plant data",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl"],
    max_active_runs=1,
    concurrency=1,
) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("ingestion") as ingestion:
        fetch_nuclear = PythonOperator(
            task_id="fetch_nuclear_metadata",
            python_callable=fetch_nuclear_metadata,
        )
        fetch_thermal = PythonOperator(
            task_id="fetch_thermal_metadata",
            python_callable=fetch_thermal_metadata,
        )
        fetch_death = PythonOperator(
            task_id="fetch_death_records_list",
            python_callable=fetch_death_records_list,
        )
        fetch_city = PythonOperator(
            task_id="fetch_city_coordinates",
            python_callable=fetch_city_coordinates,
        )

        download_nuclear = PythonOperator(
            task_id="download_nuclear_csv",
            python_callable=download_nuclear_csv,
        )
        download_thermal = PythonOperator(
            task_id="download_thermal_csv",
            python_callable=download_thermal_csv,
        )
        download_death = PythonOperator(
            task_id="download_death_records",
            python_callable=download_death_records,
        )

        fetch_nuclear >> download_nuclear
        fetch_thermal >> download_thermal
        fetch_death >> download_death
        fetch_city  # no downstream dependency

    create_deaths = PythonOperator(
        task_id="create_deaths_table",
        python_callable=create_deaths_table,
    )
    create_power_plants = PythonOperator(
        task_id="create_power_plants_table",
        python_callable=create_power_plants_table,
    )

    with TaskGroup("processing") as processing:
        load_deaths_redis = PythonOperator(
            task_id="load_death_records_to_redis",
            python_callable=load_death_records_to_redis,
        )
        clean_thermal = PythonOperator(
            task_id="clean_thermal_data",
            python_callable=clean_thermal_data,
        )
        clean_nuclear = PythonOperator(
            task_id="clean_nuclear_data",
            python_callable=clean_nuclear_data,
        )

        [download_nuclear, download_thermal, download_death] >> [load_deaths_redis, clean_thermal, clean_nuclear]

    branch = BranchPythonOperator(
        task_id="branch_on_death_data",
        python_callable=branch_on_death_data,
    )

    skip_death = DummyOperator(task_id="skip_death_processing")

    with TaskGroup("death_processing") as death_processing:
        cleanse_deaths = PythonOperator(
            task_id="cleanse_death_records",
            python_callable=cleanse_death_records,
        )
        store_deaths = PythonOperator(
            task_id="store_death_records_pg",
            python_callable=store_death_records_pg,
        )
        clean_temp_files = PythonOperator(
            task_id="clean_temp_death_files",
            python_callable=clean_temp_death_files,
        )

        cleanse_deaths >> store_deaths >> clean_temp_files

    death_path_done = DummyOperator(task_id="death_path_done")

    with TaskGroup("power_processing") as power_processing:
        generate_sql = PythonOperator(
            task_id="generate_power_plant_sql",
            python_callable=generate_power_plant_sql,
        )
        store_power = PythonOperator(
            task_id="store_power_plant_records_pg",
            python_callable=store_power_plant_records_pg,
        )

        generate_sql >> store_power

    staging_complete = DummyOperator(task_id="staging_complete")

    # Define dependencies
    start >> ingestion
    ingestion >> [create_deaths, create_power_plants]
    [create_deaths, create_power_plants] >> processing
    processing >> branch
    branch >> [death_processing.cleanse_death_records, skip_death]
    death_processing.cleanse_death_records >> death_path_done
    skip_death >> death_path_done
    processing >> power_processing.generate_power_plant_sql
    [death_path_done, power_processing.store_power_plant_records_pg] >> staging_complete

    staging_complete >> DummyOperator(task_id="end")