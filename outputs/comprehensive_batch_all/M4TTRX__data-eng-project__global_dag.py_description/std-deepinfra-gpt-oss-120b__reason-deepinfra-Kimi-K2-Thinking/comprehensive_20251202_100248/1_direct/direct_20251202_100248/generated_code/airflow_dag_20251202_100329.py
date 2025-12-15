import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook


def fetch_nuclear_metadata(**kwargs):
    """Fetch nuclear power plant metadata JSON and save to local file."""
    import requests

    url = "https://example.com/nuclear_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = "/tmp/nuclear_metadata.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(response.text)
    kwargs["ti"].xcom_push(key="nuclear_metadata_path", value=path)


def fetch_thermal_metadata(**kwargs):
    """Fetch thermal power plant metadata JSON and save to local file."""
    import requests

    url = "https://example.com/thermal_metadata.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = "/tmp/thermal_metadata.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(response.text)
    kwargs["ti"].xcom_push(key="thermal_metadata_path", value=path)


def fetch_death_resource_list(**kwargs):
    """Fetch death records resource list JSON and save to local file."""
    import requests

    url = "https://example.com/death_resources.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = "/tmp/death_resources.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(response.text)
    kwargs["ti"].xcom_push(key="death_resources_path", value=path)


def fetch_city_coordinates(**kwargs):
    """Fetch city geographic coordinates CSV and save to local file."""
    import requests

    url = "https://example.com/city_coordinates.csv"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    path = "/tmp/city_coordinates.csv"
    with open(path, "wb") as f:
        f.write(response.content)
    kwargs["ti"].xcom_push(key="city_coordinates_path", value=path)


def download_nuclear_csv(**kwargs):
    """Download nuclear CSV data using metadata."""
    import csv
    import requests

    meta_path = kwargs["ti"].xcom_pull(key="nuclear_metadata_path")
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = f.read()
    # Assume meta contains a direct CSV URL (placeholder)
    csv_url = "https://example.com/nuclear_data.csv"
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    out_path = "/tmp/nuclear_data.csv"
    with open(out_path, "wb") as f:
        f.write(response.content)
    kwargs["ti"].xcom_push(key="nuclear_csv_path", value=out_path)


def download_thermal_csv(**kwargs):
    """Download thermal CSV data using metadata."""
    import requests

    meta_path = kwargs["ti"].xcom_pull(key="thermal_metadata_path")
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = f.read()
    csv_url = "https://example.com/thermal_data.csv"
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    out_path = "/tmp/thermal_data.csv"
    with open(out_path, "wb") as f:
        f.write(response.content)
    kwargs["ti"].xcom_push(key="thermal_csv_path", value=out_path)


def download_death_files(**kwargs):
    """Download death record files based on resource list."""
    import json
    import requests

    res_path = kwargs["ti"].xcom_pull(key="death_resources_path")
    with open(res_path, "r", encoding="utf-8") as f:
        resources = json.load(f)

    # Placeholder: assume resources is a list of URLs
    downloaded_paths = []
    for idx, url in enumerate(resources.get("files", [])):
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        out_path = f"/tmp/death_record_{idx}.csv"
        with open(out_path, "wb") as f:
            f.write(resp.content)
        downloaded_paths.append(out_path)

    kwargs["ti"].xcom_push(key="death_files_paths", value=downloaded_paths)


def create_deaths_table(**kwargs):
    """Create deaths table in PostgreSQL if not exists."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    CREATE TABLE IF NOT EXISTS deaths (
        id SERIAL PRIMARY KEY,
        death_date DATE,
        city_code VARCHAR(10),
        age INT,
        gender VARCHAR(10)
    );
    """
    pg.run(sql)


def create_power_plants_table(**kwargs):
    """Create power_plants table in PostgreSQL if not exists."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    CREATE TABLE IF NOT EXISTS power_plants (
        id SERIAL PRIMARY KEY,
        plant_name VARCHAR(255),
        plant_type VARCHAR(50),
        capacity_mw NUMERIC,
        city_code VARCHAR(10)
    );
    """
    pg.run(sql)


def load_death_records_to_redis(**kwargs):
    """Load raw death record files into Redis for intermediate processing."""
    redis = RedisHook(redis_conn_id="redis_default")
    client = redis.get_conn()
    paths = kwargs["ti"].xcom_pull(key="death_files_paths")
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            data = f.read()
        key = f"death_raw:{os.path.basename(path)}"
        client.set(key, data)


def clean_thermal_data(**kwargs):
    """Clean thermal plant CSV data (rename columns, filter rows)."""
    import pandas as pd

    path = kwargs["ti"].xcom_pull(key="thermal_csv_path")
    df = pd.read_csv(path)
    df = df.rename(columns={"capacity": "capacity_mw", "type": "plant_type"})
    df = df[df["capacity_mw"] > 0]
    clean_path = "/tmp/thermal_clean.csv"
    df.to_csv(clean_path, index=False)
    kwargs["ti"].xcom_push(key="thermal_clean_path", value=clean_path)


def clean_nuclear_data(**kwargs):
    """Clean nuclear plant CSV data (rename columns, filter rows)."""
    import pandas as pd

    path = kwargs["ti"].xcom_pull(key="nuclear_csv_path")
    df = pd.read_csv(path)
    df = df.rename(columns={"capacity": "capacity_mw", "type": "plant_type"})
    df = df[df["capacity_mw"] > 0]
    clean_path = "/tmp/nuclear_clean.csv"
    df.to_csv(clean_path, index=False)
    kwargs["ti"].xcom_push(key="nuclear_clean_path", value=clean_path)


def check_death_data(**kwargs):
    """Branch: if any death file exists and is non‑empty, continue processing; else skip."""
    paths = kwargs["ti"].xcom_pull(key="death_files_paths") or []
    for p in paths:
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            return "death_data_processing.cleanse_death_records"
    return "staging_complete"


def cleanse_death_records(**kwargs):
    """Cleanse death records: date formatting and geographic enrichment."""
    import pandas as pd

    paths = kwargs["ti"].xcom_pull(key="death_files_paths")
    enriched_frames = []
    city_path = kwargs["ti"].xcom_pull(key="city_coordinates_path")
    city_df = pd.read_csv(city_path)

    for p in paths:
        df = pd.read_csv(p)
        df["death_date"] = pd.to_datetime(df["death_date"], errors="coerce")
        df = df.merge(city_df, how="left", left_on="city_code", right_on="insee_code")
        enriched_frames.append(df)

    result = pd.concat(enriched_frames, ignore_index=True)
    clean_path = "/tmp/death_clean.csv"
    result.to_csv(clean_path, index=False)
    kwargs["ti"].xcom_push(key="death_clean_path", value=clean_path)


def store_death_records_pg(**kwargs):
    """Store cleansed death records into PostgreSQL."""
    import pandas as pd

    pg = PostgresHook(postgres_conn_id="postgres_default")
    clean_path = kwargs["ti"].xcom_pull(key="death_clean_path")
    df = pd.read_csv(clean_path)
    # Use pandas to_sql for simplicity; assumes SQLAlchemy engine is configured.
    engine = pg.get_sqlalchemy_engine()
    df.to_sql("deaths", con=engine, if_exists="append", index=False)


def clean_temp_death_files(**kwargs):
    """Remove temporary death record files."""
    paths = kwargs["ti"].xcom_pull(key="death_files_paths") or []
    for p in paths:
        try:
            os.remove(p)
        except OSError:
            pass
    # Also remove intermediate raw keys from Redis
    redis = RedisHook(redis_conn_id="redis_default")
    client = redis.get_conn()
    for p in paths:
        key = f"death_raw:{os.path.basename(p)}"
        client.delete(key)


def generate_power_plant_sql(**kwargs):
    """Generate SQL statements for power plant data persistence."""
    import pandas as pd

    thermal_path = kwargs["ti"].xcom_pull(key="thermal_clean_path")
    nuclear_path = kwargs["ti"].xcom_pull(key="nuclear_clean_path")
    df_thermal = pd.read_csv(thermal_path)
    df_nuclear = pd.read_csv(nuclear_path)
    combined = pd.concat([df_thermal, df_nuclear], ignore_index=True)
    sql_path = "/tmp/power_plants_insert.sql"
    with open(sql_path, "w", encoding="utf-8") as f:
        for _, row in combined.iterrows():
            cols = ", ".join(row.index)
            vals = ", ".join([f"'{str(v).replace(\"'\", \"''\")}'" if isinstance(v, str) else str(v) for v in row.values])
            f.write(f"INSERT INTO power_plants ({cols}) VALUES ({vals});\n")
    kwargs["ti"].xcom_push(key="power_plants_sql_path", value=sql_path)


def store_power_plant_records_pg(**kwargs):
    """Execute generated SQL to store power plant records in PostgreSQL."""
    pg = PostgresHook(postgres_conn_id="postgres_default")
    sql_path = kwargs["ti"].xcom_pull(key="power_plants_sql_path")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_statements = f.read()
    pg.run(sql_statements)


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "start_date": datetime(2024, 1, 1),
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
            task_id="fetch_nuclear_metadata", python_callable=fetch_nuclear_metadata
        )
        fetch_thermal = PythonOperator(
            task_id="fetch_thermal_metadata", python_callable=fetch_thermal_metadata
        )
        fetch_death = PythonOperator(
            task_id="fetch_death_resource_list", python_callable=fetch_death_resource_list
        )
        fetch_city = PythonOperator(
            task_id="fetch_city_coordinates", python_callable=fetch_city_coordinates
        )

        download_nuclear = PythonOperator(
            task_id="download_nuclear_csv", python_callable=download_nuclear_csv
        )
        download_thermal = PythonOperator(
            task_id="download_thermal_csv", python_callable=download_thermal_csv
        )
        download_death = PythonOperator(
            task_id="download_death_files", python_callable=download_death_files
        )

        fetch_nuclear >> download_nuclear
        fetch_thermal >> download_thermal
        fetch_death >> download_death
        # city coordinates are independent; no downstream dependency here.

    with TaskGroup(group_id="table_creation") as tables:
        create_deaths = PythonOperator(
            task_id="create_deaths_table", python_callable=create_deaths_table
        )
        create_power_plants = PythonOperator(
            task_id="create_power_plants_table", python_callable=create_power_plants_table
        )

    with TaskGroup(group_id="processing") as processing:
        load_death_redis = PythonOperator(
            task_id="load_death_records_to_redis", python_callable=load_death_records_to_redis
        )
        clean_thermal = PythonOperator(
            task_id="clean_thermal_data", python_callable=clean_thermal_data
        )
        clean_nuclear = PythonOperator(
            task_id="clean_nuclear_data", python_callable=clean_nuclear_data
        )

        [download_death, create_deaths] >> load_death_redis
        [download_thermal, create_power_plants] >> clean_thermal
        [download_nuclear, create_power_plants] >> clean_nuclear

    check_death = BranchPythonOperator(
        task_id="check_death_data", python_callable=check_death_data
    )

    with TaskGroup(group_id="death_data_processing") as death_processing:
        cleanse_death = PythonOperator(
            task_id="cleanse_death_records", python_callable=cleanse_death_records
        )
        store_death = PythonOperator(
            task_id="store_death_records_pg", python_callable=store_death_records_pg
        )
        clean_temp = PythonOperator(
            task_id="clean_temp_death_files", python_callable=clean_temp_death_files
        )
        cleanse_death >> store_death >> clean_temp

    with TaskGroup(group_id="power_plant_processing") as plant_processing:
        generate_sql = PythonOperator(
            task_id="generate_power_plant_sql", python_callable=generate_power_plant_sql
        )
        store_plant = PythonOperator(
            task_id="store_power_plant_records_pg", python_callable=store_power_plant_records_pg
        )
        generate_sql >> store_plant

    staging_complete = DummyOperator(task_id="staging_complete")

    # Define DAG flow
    start >> ingestion >> tables >> processing >> check_death

    # Branch outcomes
    check_death >> death_processing
    check_death >> staging_complete

    # Power plant processing runs irrespective of death branch
    processing >> plant_processing

    # Final synchronization
    [plant_processing, death_processing] >> staging_complete