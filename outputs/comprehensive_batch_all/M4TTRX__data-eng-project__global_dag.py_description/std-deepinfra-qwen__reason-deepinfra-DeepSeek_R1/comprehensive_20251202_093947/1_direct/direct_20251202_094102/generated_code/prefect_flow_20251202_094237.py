from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.task_runners import SequentialTaskRunner
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
    logger.info("Fetching nuclear power plant metadata...")
    response = requests.get("https://data.gouv.fr/api/1/datasets/nuclear-power-plants/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_thermal_power_plant_metadata():
    logger = get_run_logger()
    logger.info("Fetching thermal power plant metadata...")
    response = requests.get("https://data.gouv.fr/api/1/datasets/thermal-power-plants/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_death_records_resource_list():
    logger = get_run_logger()
    logger.info("Fetching death records resource list...")
    response = requests.get("https://data.gouv.fr/api/1/datasets/death-records/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def fetch_city_geographic_coordinates():
    logger = get_run_logger()
    logger.info("Fetching city geographic coordinates...")
    response = requests.get("https://data.gouv.fr/api/1/datasets/city-geographic-coordinates/")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def download_nuclear_power_plant_data(metadata):
    logger = get_run_logger()
    logger.info("Downloading nuclear power plant data...")
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    with open("nuclear_power_plant_data.csv", "wb") as f:
        f.write(response.content)
    return "nuclear_power_plant_data.csv"

@task(retries=3, retry_delay_seconds=10)
def download_thermal_power_plant_data(metadata):
    logger = get_run_logger()
    logger.info("Downloading thermal power plant data...")
    url = metadata['resources'][0]['url']
    response = requests.get(url)
    response.raise_for_status()
    with open("thermal_power_plant_data.csv", "wb") as f:
        f.write(response.content)
    return "thermal_power_plant_data.csv"

@task(retries=3, retry_delay_seconds=10)
def download_death_record_files(resource_list):
    logger = get_run_logger()
    logger.info("Downloading death record files...")
    for resource in resource_list['resources']:
        url = resource['url']
        response = requests.get(url)
        response.raise_for_status()
        filename = os.path.basename(url)
        with open(filename, "wb") as f:
            f.write(response.content)
    return [os.path.basename(resource['url']) for resource in resource_list['resources']]

@task(retries=3, retry_delay_seconds=10)
def create_deaths_table():
    logger = get_run_logger()
    logger.info("Creating deaths table...")
    conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host")
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

@task(retries=3, retry_delay_seconds=10)
def create_power_plants_table():
    logger = get_run_logger()
    logger.info("Creating power plants table...")
    conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host")
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
def load_death_records_into_redis(filenames):
    logger = get_run_logger()
    logger.info("Loading death records into Redis...")
    r = redis.Redis(host='localhost', port=6379, db=0)
    for filename in filenames:
        df = pd.read_csv(filename)
        for index, row in df.iterrows():
            r.set(f"death:{index}", row.to_json())
    return filenames

@task(retries=3, retry_delay_seconds=10)
def clean_thermal_plant_data(filename):
    logger = get_run_logger()
    logger.info("Cleaning thermal plant data...")
    df = pd.read_csv(filename)
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity", "plant_location": "location"})
    df = df[df['capacity'] > 0]
    return df

@task(retries=3, retry_delay_seconds=10)
def clean_nuclear_plant_data(filename):
    logger = get_run_logger()
    logger.info("Cleaning nuclear plant data...")
    df = pd.read_csv(filename)
    df = df.rename(columns={"plant_name": "name", "plant_type": "type", "plant_capacity": "capacity", "plant_location": "location"})
    df = df[df['capacity'] > 0]
    return df

@task(retries=3, retry_delay_seconds=10)
def check_death_data_availability(filenames):
    logger = get_run_logger()
    logger.info("Checking death data availability...")
    return len(filenames) > 0

@task(retries=3, retry_delay_seconds=10)
def cleanse_death_records(filenames):
    logger = get_run_logger()
    logger.info("Cleansing death records...")
    r = redis.Redis(host='localhost', port=6379, db=0)
    for filename in filenames:
        df = pd.read_csv(filename)
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna(subset=['city', 'insee_code'])
        df = df.merge(pd.read_csv("city_geographic_coordinates.csv"), on='insee_code')
        for index, row in df.iterrows():
            r.set(f"death:{index}", row.to_json())
    return filenames

@task(retries=3, retry_delay_seconds=10)
def store_cleansed_death_records_in_postgresql(filenames):
    logger = get_run_logger()
    logger.info("Storing cleansed death records in PostgreSQL...")
    conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host")
    cur = conn.cursor()
    r = redis.Redis(host='localhost', port=6379, db=0)
    for filename in filenames:
        df = pd.read_csv(filename)
        for index, row in df.iterrows():
            cur.execute("""
                INSERT INTO deaths (date, city, insee_code, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s);
            """, (row['date'], row['city'], row['insee_code'], row['latitude'], row['longitude']))
    conn.commit()
    cur.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10)
def clean_temporary_death_data_files(filenames):
    logger = get_run_logger()
    logger.info("Cleaning temporary death data files...")
    for filename in filenames:
        os.remove(filename)

@task(retries=3, retry_delay_seconds=10)
def generate_sql_queries_for_plant_data_persistence(df, table_name):
    logger = get_run_logger()
    logger.info(f"Generating SQL queries for {table_name} data persistence...")
    sql_queries = []
    for index, row in df.iterrows():
        sql_queries.append(f"""
            INSERT INTO {table_name} (name, type, capacity, location, latitude, longitude)
            VALUES ('{row['name']}', '{row['type']}', {row['capacity']}, '{row['location']}', {row['latitude']}, {row['longitude']});
        """)
    return sql_queries

@task(retries=3, retry_delay_seconds=10)
def store_power_plant_records_in_postgresql(sql_queries, table_name):
    logger = get_run_logger()
    logger.info(f"Storing {table_name} records in PostgreSQL...")
    conn = psycopg2.connect("dbname=your_db user=your_user password=your_password host=your_host")
    cur = conn.cursor()
    for query in sql_queries:
        cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# Flow definition

@flow(task_runner=SequentialTaskRunner())
def etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting ETL pipeline...")

    # Parallel data ingestion
    nuclear_metadata = fetch_nuclear_power_plant_metadata.submit()
    thermal_metadata = fetch_thermal_power_plant_metadata.submit()
    death_records_list = fetch_death_records_resource_list.submit()
    city_coordinates = fetch_city_geographic_coordinates.submit()

    # Process fetched metadata
    nuclear_data_file = download_nuclear_power_plant_data.submit(nuclear_metadata)
    thermal_data_file = download_thermal_power_plant_data.submit(thermal_metadata)
    death_record_files = download_death_record_files.submit(death_records_list)

    # Create PostgreSQL tables
    create_deaths_table()
    create_power_plants_table()

    # Parallel data processing
    load_death_records_into_redis.submit(death_record_files)
    clean_thermal_data = clean_thermal_plant_data.submit(thermal_data_file)
    clean_nuclear_data = clean_nuclear_plant_data.submit(nuclear_data_file)

    # Branch based on death data availability
    death_data_available = check_death_data_availability(death_record_files)

    if death_data_available:
        cleansed_death_records = cleanse_death_records(death_record_files)
        store_cleansed_death_records_in_postgresql(cleansed_death_records)
        clean_temporary_death_data_files(cleansed_death_records)

    # Power plant data processing
    thermal_sql_queries = generate_sql_queries_for_plant_data_persistence(clean_thermal_data, "power_plants")
    nuclear_sql_queries = generate_sql_queries_for_plant_data_persistence(clean_nuclear_data, "power_plants")

    store_power_plant_records_in_postgresql(thermal_sql_queries, "power_plants")
    store_power_plant_records_in_postgresql(nuclear_sql_queries, "power_plants")

    logger.info("ETL pipeline completed.")

if __name__ == '__main__':
    etl_pipeline()