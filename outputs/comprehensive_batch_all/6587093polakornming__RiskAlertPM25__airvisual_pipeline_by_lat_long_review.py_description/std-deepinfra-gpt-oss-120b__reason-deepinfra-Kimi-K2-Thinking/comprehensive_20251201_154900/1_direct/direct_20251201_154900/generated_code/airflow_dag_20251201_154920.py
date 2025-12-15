import os
import json
import logging
import configparser
from datetime import datetime, timedelta

import requests
import pytz
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "airvisual_data.json")
TEMP_DATA_FILE = DATA_FILE + ".tmp"
MAPPINGS_DIR = os.path.join(BASE_DIR, "mappings")
CONFIG_FILE = os.path.join(BASE_DIR, "config.conf")

# Coordinates for Salaya, Nakhon Pathom, Thailand
LATITUDE = 13.8235
LONGITUDE = 100.0015
TIMEZONE = pytz.timezone("Asia/Bangkok")

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def load_config():
    """Load configuration values from config.conf."""
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    api_key = parser.get("airvisual", "api_key", fallback=os.getenv("AIRVISUAL_API_KEY"))
    return {"api_key": api_key}


def fetch_airvisual_data():
    """Call AirVisual API and return JSON payload."""
    cfg = load_config()
    url = (
        "http://api.airvisual.com/v2/nearest_city?"
        f"lat={LATITUDE}&lon={LONGITUDE}&key={cfg['api_key']}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def convert_timestamp_to_bangkok(ts_str):
    """Convert ISO timestamp string to Bangkok timezone aware datetime."""
    utc_dt = datetime.fromisoformat(ts_str.rstrip("Z")).replace(tzinfo=pytz.UTC)
    return utc_dt.astimezone(TIMEZONE)


def data_exists_in_db(pg_hook, ts):
    """Check if a record for the given hour already exists."""
    sql = """
        SELECT 1
        FROM factairvisualtable
        WHERE date_trunc('hour', timestamp) = date_trunc('hour', %s)
        LIMIT 1;
    """
    result = pg_hook.get_first(sql, parameters=(ts,))
    return result is not None


def atomic_write_json(data, final_path, temp_path):
    """Write JSON data atomically."""
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, final_path)


# ----------------------------------------------------------------------
# Task functions
# ----------------------------------------------------------------------
def get_airvisual_data_hourly(**context):
    """
    Fetch AirVisual data, validate, deduplicate, and store JSON atomically.
    Skips downstream tasks if data for the current hour already exists.
    """
    logging.info("Fetching AirVisual data for coordinates (%s, %s)", LATITUDE, LONGITUDE)
    payload = fetch_airvisual_data()

    # Basic validation
    if "data" not in payload:
        raise ValueError("Invalid API response: missing 'data' field")

    # Convert timestamp
    ts_str = payload["data"]["current"]["pollution"]["ts"]
    ts = convert_timestamp_to_bangkok(ts_str)

    # Deduplication check
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    if data_exists_in_db(pg_hook, ts):
        logging.info("Data for hour %s already exists. Skipping.", ts)
        raise AirflowSkipException("Duplicate hour data")

    # Store JSON atomically
    atomic_write_json(payload, DATA_FILE, TEMP_DATA_FILE)
    logging.info("AirVisual data saved to %s", DATA_FILE)

    # Push timestamp to XCom for downstream use
    context["ti"].xcom_push(key="airvisual_timestamp", value=ts.isoformat())


def read_data_airvisual(**context):
    """
    Read the saved JSON file and validate its structure.
    """
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Simple structure validation
    required_keys = ["data", "status"]
    for key in required_keys:
        if key not in data:
            raise ValueError(f"Missing required key '{key}' in AirVisual data")

    logging.info("AirVisual data successfully read and validated.")
    # Pass data forward via XCom if needed
    context["ti"].xcom_push(key="airvisual_data", value=data)


def load_airvisual_to_postgresql(**context):
    """
    Perform ETL: load AirVisual data into PostgreSQL dimension and fact tables.
    """
    # Retrieve data from XCom (fallback to file read if not present)
    ti = context["ti"]
    data = ti.xcom_pull(key="airvisual_data", task_ids="read_data_airvisual")
    if not data:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    # Load mapping configurations
    pollution_map_path = os.path.join(MAPPINGS_DIR, "pollution_mapping.json")
    weather_map_path = os.path.join(MAPPINGS_DIR, "weather_mapping.json")
    with open(pollution_map_path, "r", encoding="utf-8") as f:
        pollution_mapping = json.load(f)
    with open(weather_map_path, "r", encoding="utf-8") as f:
        weather_mapping = json.load(f)

    # Extract relevant fields
    current = data["data"]["current"]
    pollution = current["pollution"]
    weather = current["weather"]
    ts = convert_timestamp_to_bangkok(pollution["ts"])

    # Prepare insert statements with conflict handling
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            # dimDateTimeTable
            cur.execute(
                """
                INSERT INTO dimDateTimeTable (timestamp)
                VALUES (%s)
                ON CONFLICT (timestamp) DO UPDATE SET timestamp = EXCLUDED.timestamp;
                """,
                (ts,),
            )

            # dimLocationTable (example columns)
            cur.execute(
                """
                INSERT INTO dimLocationTable (latitude, longitude, city, country)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (latitude, longitude) DO NOTHING;
                """,
                (
                    LATITUDE,
                    LONGITUDE,
                    data["data"]["city"],
                    data["data"]["country"],
                ),
            )

            # dimMainPollutionTable
            pollution_type = pollution_mapping.get(str(pollution["aqius"]), "unknown")
            cur.execute(
                """
                INSERT INTO dimMainPollutionTable (pollution_type, aqi_us, aqi_cn)
                VALUES (%s, %s, %s)
                ON CONFLICT (pollution_type) DO UPDATE
                SET aqi_us = EXCLUDED.aqi_us,
                    aqi_cn = EXCLUDED.aqi_cn;
                """,
                (pollution_type, pollution["aqius"], pollution["aqicn"]),
            )

            # factairvisualtable
            weather_desc = weather_mapping.get(str(weather["ic"]), "unknown")
            cur.execute(
                """
                INSERT INTO factairvisualtable (
                    timestamp,
                    location_id,
                    pollution_id,
                    temperature,
                    humidity,
                    pressure,
                    weather_description
                )
                VALUES (
                    %s,
                    (SELECT id FROM dimLocationTable WHERE latitude = %s AND longitude = %s),
                    (SELECT id FROM dimMainPollutionTable WHERE pollution_type = %s),
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (timestamp, location_id) DO UPDATE
                SET temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    weather_description = EXCLUDED.weather_description;
                """,
                (
                    ts,
                    LATITUDE,
                    LONGITUDE,
                    pollution_type,
                    weather["tp"],
                    weather["hu"],
                    weather["pr"],
                    weather_desc,
                ),
            )
        conn.commit()
        logging.info("AirVisual data loaded into PostgreSQL successfully.")
    except Exception as exc:
        conn.rollback()
        logging.error("Error loading data to PostgreSQL: %s", exc)
        raise


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="airvisual_etl",
    description="ETL pipeline for AirVisual data into PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["airquality", "etl"],
) as dag:

    fetch_task = PythonOperator(
        task_id="get_airvisual_data_hourly",
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    read_task = PythonOperator(
        task_id="read_data_airvisual",
        python_callable=read_data_airvisual,
    )

    load_task = PythonOperator(
        task_id="load_airvisual_to_postgresql",
        python_callable=load_airvisual_to_postgresql,
    )

    fetch_task >> read_task >> load_task