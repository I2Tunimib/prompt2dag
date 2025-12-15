from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.futures import PrefectFuture
from typing import List
import requests
import pandas as pd
import psycopg2
import redis
import os

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def fetch_nuclear_power_plant_metadata() -> dict:
    """Fetch nuclear power plant metadata from public API."""
    response = requests.get("https://data.gouv.fr/api/1/datasets/nuclear-power-plants")
    response.raise_for_status()
    return response.json()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def fetch_thermal_power_plant_metadata() -> dict:
    """Fetch thermal power plant metadata from public API."""
    response = requests.get("https://data.gouv.fr/api/1/datasets/thermal-power-plants")
    response.raise_for_status()
    return response.json()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def fetch_death_records_resource_list() -> dict:
    """Fetch death records resource list from public API."""
    response = requests.get("https://data.gouv.fr/api/1/datasets/death-records")
    response.raise_for_status()
    return response.json()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def fetch_city_geographic_coordinates_csv() -> pd.DataFrame:
    """Fetch city geographic coordinates CSV from public API."""
    response = requests.get("https://data.gouv.fr/api/1/datasets/city-geographic-coordinates")
    response.raise_for_status()
    return pd.read_csv(response.content)

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def download_nuclear_power_plant_csv(metadata: dict) -> pd.DataFrame:
    """Download nuclear power plant CSV data from metadata."""
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(response.content)

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def download_thermal_power_plant_csv(metadata: dict) -> pd.DataFrame:
    """Download thermal power plant CSV data from metadata."""
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(response.content)

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def download_death_record_files(resource_list: dict) -> List[str]:
    """Download death record files from resource list."""
    files = []
    for resource in resource_list['resources']:
        url = resource['url']
        response = requests.get(url)
        response.raise_for_status()
        file_path = f"death_records_{resource['id']}.csv"
        with open(file_path, 'wb') as f:
            f.write(response.content)
        files.append(file_path)
    return files

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def create_deaths_table():
    """Create deaths table in PostgreSQL."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deaths (
            id SERIAL PRIMARY KEY,
            date DATE,
            city VARCHAR(255),
            insee_code VARCHAR(5),
            latitude FLOAT,
            longitude FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def create_power_plants_table():
    """Create power_plants table in PostgreSQL."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS power_plants (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(50),
            capacity FLOAT,
            latitude FLOAT,
            longitude FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def load_death_records_into_redis(files: List[str]):
    """Load death records from ingestion files into Redis."""
    r = redis.Redis(host='localhost', port=6379, db=0)
    for file in files:
        df = pd.read_csv(file)
        for index, row in df.iterrows():
            r.hset(f"death_record:{index}", mapping=row.to_dict())

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def clean_thermal_plant_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean thermal plant data (column renaming, filtering)."""
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity"})
    df = df[df['type'].isin(['coal', 'gas', 'oil'])]
    return df

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def clean_nuclear_plant_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean nuclear plant data (column renaming, filtering)."""
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity"})
    df = df[df['type'] == 'nuclear']
    return df

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def cleanse_death_records(df: pd.DataFrame, city_coordinates: pd.DataFrame) -> pd.DataFrame:
    """Cleanse death records (date formatting, geographic enrichment)."""
    df['date'] = pd.to_datetime(df['date'])
    df = pd.merge(df, city_coordinates, on='insee_code', how='left')
    return df

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def store_cleansed_death_records_in_postgresql(df: pd.DataFrame):
    """Store cleansed death records in PostgreSQL."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    for index, row in df.iterrows():
        cur.execute("""
            INSERT INTO deaths (date, city, insee_code, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s);
        """, (row['date'], row['city'], row['insee_code'], row['latitude'], row['longitude']))
    conn.commit()
    cur.close()
    conn.close()

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def clean_temporary_death_data_files(files: List[str]):
    """Clean temporary death data files."""
    for file in files:
        os.remove(file)

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def generate_sql_queries_for_plant_data_persistence(df: pd.DataFrame) -> List[str]:
    """Generate SQL queries for plant data persistence."""
    queries = []
    for index, row in df.iterrows():
        query = f"""
            INSERT INTO power_plants (name, type, capacity, latitude, longitude)
            VALUES ('{row['name']}', '{row['type']}', {row['capacity']}, {row['latitude']}, {row['longitude']});
        """
        queries.append(query)
    return queries

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def store_power_plant_records_in_postgresql(queries: List[str]):
    """Store power plant records in PostgreSQL."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    for query in queries:
        cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

@flow(name="ETL Pipeline for French Government Data")
def etl_pipeline():
    logger = get_run_logger()

    # Parallel data ingestion
    nuclear_metadata = fetch_nuclear_power_plant_metadata.submit()
    thermal_metadata = fetch_thermal_power_plant_metadata.submit()
    death_records_list = fetch_death_records_resource_list.submit()
    city_coordinates = fetch_city_geographic_coordinates_csv.submit()

    # Process fetched metadata
    nuclear_data = download_nuclear_power_plant_csv.submit(nuclear_metadata)
    thermal_data = download_thermal_power_plant_csv.submit(thermal_metadata)
    death_files = download_death_record_files.submit(death_records_list)

    # Create PostgreSQL tables
    create_deaths_table()
    create_power_plants_table()

    # Parallel data processing
    load_death_records_into_redis.submit(death_files)
    clean_thermal_data = clean_thermal_plant_data.submit(thermal_data)
    clean_nuclear_data = clean_nuclear_plant_data.submit(nuclear_data)

    # Branch based on death data availability
    if death_files.result():
        # Death data processing path
        death_df = pd.concat([pd.read_csv(file) for file in death_files.result()])
        city_coordinates_df = city_coordinates.result()
        cleansed_death_records = cleanse_death_records.submit(death_df, city_coordinates_df)
        store_cleansed_death_records_in_postgresql.submit(cleansed_death_records)
        clean_temporary_death_data_files.submit(death_files)

    # Power plant data processing
    thermal_queries = generate_sql_queries_for_plant_data_persistence.submit(clean_thermal_data)
    nuclear_queries = generate_sql_queries_for_plant_data_persistence.submit(clean_nuclear_data)
    store_power_plant_records_in_postgresql.submit(thermal_queries)
    store_power_plant_records_in_postgresql.submit(nuclear_queries)

    logger.info("ETL pipeline completed successfully.")

if __name__ == '__main__':
    etl_pipeline()