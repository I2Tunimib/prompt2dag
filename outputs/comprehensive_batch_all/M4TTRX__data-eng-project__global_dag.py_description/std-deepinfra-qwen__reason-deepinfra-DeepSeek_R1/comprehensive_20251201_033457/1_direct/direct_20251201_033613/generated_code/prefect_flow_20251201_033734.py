from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
import pandas as pd
import psycopg2
import redis
import os

# Task definitions
@task(retries=3, retry_delay_seconds=10)
def fetch_nuclear_power_plant_metadata():
    logger = get_run_logger()
    logger.info("Fetching nuclear power plant metadata")
    response = requests.get("https://data.gouv.fr/api/1/datasets/nuclear-power-plants/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_power_plant_metadata():
    logger = get_run_logger()
    logger.info("Fetching thermal power plant metadata")
    response = requests.get("https://data.gouv.fr/api/1/datasets/thermal-power-plants/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_death_records_resource_list():
    logger = get_run_logger()
    logger.info("Fetching death records resource list")
    response = requests.get("https://data.gouv.fr/api/1/datasets/death-records/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_city_geographic_coordinates_csv():
    logger = get_run_logger()
    logger.info("Fetching city geographic coordinates CSV")
    response = requests.get("https://data.gouv.fr/api/1/datasets/city-geographic-coordinates/")
    response.raise_for_status()
    return response.content

@task(retries=3, retry_delay_seconds=10)
def download_nuclear_power_plant_csv(metadata):
    logger = get_run_logger()
    logger.info("Downloading nuclear power plant CSV data")
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    return response.content

@task(retries=3, retry_delay_seconds=10)
def download_thermal_power_plant_csv(metadata):
    logger = get_run_logger()
    logger.info("Downloading thermal power plant CSV data")
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    return response.content

@task(retries=3, retry_delay_seconds=10)
def download_death_record_files(resource_list):
    logger = get_run_logger()
    logger.info("Downloading death record files")
    files = []
    for resource in resource_list['resources']:
        url = resource['url']
        response = requests.get(url)
        response.raise_for_status()
        files.append(response.content)
    return files

@task(retries=3, retry_delay_seconds=10)
def create_deaths_table():
    logger = get_run_logger()
    logger.info("Creating deaths table in PostgreSQL")
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deaths (
            id SERIAL PRIMARY KEY,
            date DATE,
            city VARCHAR(255),
            insee_code VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10)
def create_power_plants_table():
    logger = get_run_logger()
    logger.info("Creating power_plants table in PostgreSQL")
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS power_plants (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(50),
            capacity FLOAT,
            location VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10)
def load_death_records_into_redis(death_files):
    logger = get_run_logger()
    logger.info("Loading death records into Redis")
    r = redis.Redis(host='localhost', port=6379, db=0)
    for file in death_files:
        df = pd.read_csv(file)
        for index, row in df.iterrows():
            r.set(f"death:{index}", row.to_json())
    return r.keys()

@task(retries=3, retry_delay_seconds=10)
def clean_thermal_plant_data(thermal_csv):
    logger = get_run_logger()
    logger.info("Cleaning thermal plant data")
    df = pd.read_csv(thermal_csv)
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity", "plant_location": "location"})
    df = df[df['type'] == 'thermal']
    return df

@task(retries=3, retry_delay_seconds=10)
def clean_nuclear_plant_data(nuclear_csv):
    logger = get_run_logger()
    logger.info("Cleaning nuclear plant data")
    df = pd.read_csv(nuclear_csv)
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity", "plant_location": "location"})
    df = df[df['type'] == 'nuclear']
    return df

@task(retries=3, retry_delay_seconds=10)
def cleanse_death_records(redis_keys):
    logger = get_run_logger()
    logger.info("Cleansing death records")
    r = redis.Redis(host='localhost', port=6379, db=0)
    cleansed_records = []
    for key in redis_keys:
        record = pd.read_json(r.get(key))
        record['date'] = pd.to_datetime(record['date']).dt.date
        record['latitude'] = float(record['latitude'])
        record['longitude'] = float(record['longitude'])
        cleansed_records.append(record)
    return cleansed_records

@task(retries=3, retry_delay_seconds=10)
def store_cleansed_death_records(cleansed_records):
    logger = get_run_logger()
    logger.info("Storing cleansed death records in PostgreSQL")
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    for record in cleansed_records:
        cur.execute("""
            INSERT INTO deaths (date, city, insee_code, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s);
        """, (record['date'], record['city'], record['insee_code'], record['latitude'], record['longitude']))
    conn.commit()
    cur.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10)
def clean_temporary_death_data_files(death_files):
    logger = get_run_logger()
    logger.info("Cleaning temporary death data files")
    for file in death_files:
        os.remove(file)

@task(retries=3, retry_delay_seconds=10)
def generate_sql_queries_for_plant_data(plant_data):
    logger = get_run_logger()
    logger.info("Generating SQL queries for plant data")
    queries = []
    for index, row in plant_data.iterrows():
        query = f"""
            INSERT INTO power_plants (name, type, capacity, location, latitude, longitude)
            VALUES ('{row['name']}', '{row['type']}', {row['capacity']}, '{row['location']}', {row['latitude']}, {row['longitude']});
        """
        queries.append(query)
    return queries

@task(retries=3, retry_delay_seconds=10)
def store_power_plant_records(plant_queries):
    logger = get_run_logger()
    logger.info("Storing power plant records in PostgreSQL")
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    for query in plant_queries:
        cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# Flow definition
@flow(name="ETL Pipeline for French Government Data")
def etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting ETL pipeline")

    # Parallel data ingestion
    nuclear_metadata = fetch_nuclear_power_plant_metadata.submit()
    thermal_metadata = fetch_thermal_power_plant_metadata.submit()
    death_resources = fetch_death_records_resource_list.submit()
    city_coordinates = fetch_city_geographic_coordinates_csv.submit()

    # Process fetched metadata
    nuclear_csv = download_nuclear_power_plant_csv.submit(nuclear_metadata)
    thermal_csv = download_thermal_power_plant_csv.submit(thermal_metadata)
    death_files = download_death_record_files.submit(death_resources)

    # Create PostgreSQL tables
    create_deaths_table()
    create_power_plants_table()

    # Parallel data processing
    redis_keys = load_death_records_into_redis.submit(death_files)
    thermal_data = clean_thermal_plant_data.submit(thermal_csv)
    nuclear_data = clean_nuclear_plant_data.submit(nuclear_csv)

    # Branch based on death data availability
    if redis_keys.result():
        cleansed_records = cleanse_death_records.submit(redis_keys)
        store_cleansed_death_records.submit(cleansed_records)
        clean_temporary_death_data_files.submit(death_files)

    # Power plant data processing
    thermal_queries = generate_sql_queries_for_plant_data.submit(thermal_data)
    nuclear_queries = generate_sql_queries_for_plant_data.submit(nuclear_data)
    store_power_plant_records.submit(thermal_queries)
    store_power_plant_records.submit(nuclear_queries)

    logger.info("ETL pipeline completed")

if __name__ == "__main__":
    etl_pipeline()