import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import configparser
import pytz
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Paths and constants
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_FILE = os.path.join(BASE_DIR, "airvisual_data.json")
POLLUTION_MAPPING_FILE = os.path.join(BASE_DIR, "pollution_mapping.json")
WEATHER_MAPPING_FILE = os.path.join(BASE_DIR, "weather_mapping.json")
CONFIG_FILE = os.path.join(BASE_DIR, "config.conf")
TIMEZONE = pytz.timezone("Asia/Bangkok")
POSTGRES_CONN_ID = "postgres_conn"

# Load configuration (API key and coordinates)
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

API_KEY = config.get("airvisual", "api_key", fallback="")
LATITUDE = config.getfloat("airvisual", "latitude", fallback=13.7563)
LONGITUDE = config.getfloat("airvisual", "longitude", fallback=100.5018)


def get_airvisual_data_hourly(**context):
    """
    Fetch current air quality data from AirVisual API.
    - Validates response.
    - Converts timestamp to Bangkok timezone.
    - Checks for duplicate hour in PostgreSQL.
    - Writes JSON atomically to a temporary file before moving to final location.
    - Skips downstream tasks if data already exists.
    """
    logger = logging.getLogger("airvisual_etl")
    url = (
        f"https://api.airvisual.com/v2/nearest_city?"
        f"lat={LATITUDE}&lon={LONGITUDE}&key={API_KEY}"
    )
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise Exception(f"AirVisual API request failed with status {response.status_code}")

    data = response.json()
    if "data" not in data:
        raise Exception("Invalid API response structure: missing 'data' key")

    # Convert timestamp to Bangkok timezone
    utc_ts = datetime.utcfromtimestamp(data["data"]["current"]["pollution"]["ts"])
    bangkok_ts = utc_ts.replace(tzinfo=pytz.utc).astimezone(TIMEZONE)
    data["data"]["current"]["pollution"]["bangkok_ts"] = bangkok_ts.isoformat()

    # Check for duplicate hour in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        SELECT 1 FROM factairvisualtable
        WHERE date_trunc('hour', timestamp) = date_trunc('hour', %s::timestamptz)
        LIMIT 1;
    """
    records = pg_hook.get_records(sql, parameters=(bangkok_ts,))
    if records:
        logger.info("Data for hour %s already exists. Skipping downstream tasks.", bangkok_ts)
        raise AirflowSkipException

    # Atomic write to JSON file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as tmp_file:
            json.dump(data, tmp_file, ensure_ascii=False, indent=2)
        os.replace(temp_path, DATA_FILE)
        logger.info("AirVisual data written atomically to %s", DATA_FILE)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def read_data_airvisual(**context):
    """
    Read the saved JSON file and validate its structure.
    Logs a success message for monitoring.
    """
    logger = logging.getLogger("airvisual_etl")
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found at {DATA_FILE}")

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Basic validation of expected keys
    required_keys = ["data", "status"]
    for key in required_keys:
        if key not in data:
            raise ValueError(f"Missing required key '{key}' in AirVisual data")

    logger.info("AirVisual data successfully read and validated from %s", DATA_FILE)


def load_airvisual_to_postgresql(**context):
    """
    Load the AirVisual data into PostgreSQL.
    - Loads mapping configurations.
    - Inserts into dimension tables with conflict handling.
    - Inserts fact record within a transaction.
    """
    logger = logging.getLogger("airvisual_etl")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Load JSON data
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)

    pollution = payload["data"]["current"]["pollution"]
    weather = payload["data"]["current"]["weather"]
    location = payload["data"]["city"]
    timestamp = datetime.fromisoformat(pollution["bangkok_ts"])

    # Load mapping files
    with open(POLLUTION_MAPPING_FILE, "r", encoding="utf-8") as f:
        pollution_mapping = json.load(f)
    with open(WEATHER_MAPPING_FILE, "r", encoding="utf-8") as f:
        weather_mapping = json.load(f)

    # Prepare dimension records
    dim_datetime = {
        "timestamp": timestamp,
        "year": timestamp.year,
        "month": timestamp.month,
        "day": timestamp.day,
        "hour": timestamp.hour,
    }

    dim_location = {
        "city": location,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "timezone": "Asia/Bangkok",
    }

    dim_pollution = {
        "aqi_us": pollution.get("aqius"),
        "main_pollutant_us": pollution.get("mainus"),
        "aqi_cn": pollution.get("aqicn"),
        "main_pollutant_cn": pollution.get("maincn"),
    }

    # Map weather fields according to configuration
    fact_weather = {
        key: weather.get(weather_mapping.get(key, ""))
        for key in weather_mapping
    }

    # Begin transaction
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            # Insert into dimDateTimeTable
            cur.execute(
                """
                INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO UPDATE
                SET year = EXCLUDED.year,
                    month = EXCLUDED.month,
                    day = EXCLUDED.day,
                    hour = EXCLUDED.hour;
                """,
                (
                    dim_datetime["timestamp"],
                    dim_datetime["year"],
                    dim_datetime["month"],
                    dim_datetime["day"],
                    dim_datetime["hour"],
                ),
            )

            # Insert into dimLocationTable
            cur.execute(
                """
                INSERT INTO dimLocationTable (city, latitude, longitude, timezone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (city) DO UPDATE
                SET latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timezone = EXCLUDED.timezone;
                """,
                (
                    dim_location["city"],
                    dim_location["latitude"],
                    dim_location["longitude"],
                    dim_location["timezone"],
                ),
            )

            # Insert into dimMainPollutionTable
            cur.execute(
                """
                INSERT INTO dimMainPollutionTable (aqi_us, main_pollutant_us, aqi_cn, main_pollutant_cn)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (aqi_us, aqi_cn) DO UPDATE
                SET main_pollutant_us = EXCLUDED.main_pollutant_us,
                    main_pollutant_cn = EXCLUDED.main_pollutant_cn;
                """,
                (
                    dim_pollution["aqi_us"],
                    dim_pollution["main_pollutant_us"],
                    dim_pollution["aqi_cn"],
                    dim_pollution["main_pollutant_cn"],
                ),
            )

            # Insert fact record
            fact_columns = [
                "timestamp",
                "city",
                "aqi_us",
                "main_pollutant_us",
                "aqi_cn",
                "main_pollutant_cn",
                "temperature",
                "humidity",
                "pressure",
                "wind_speed",
                "wind_dir",
            ]
            fact_values = [
                timestamp,
                dim_location["city"],
                dim_pollution["aqi_us"],
                dim_pollution["main_pollutant_us"],
                dim_pollution["aqi_cn"],
                dim_pollution["main_pollutant_cn"],
                fact_weather.get("temperature"),
                fact_weather.get("humidity"),
                fact_weather.get("pressure"),
                fact_weather.get("wind_speed"),
                fact_weather.get("wind_dir"),
            ]

            insert_sql = f"""
                INSERT INTO factairvisualtable ({', '.join(fact_columns)})
                VALUES ({', '.join(['%s'] * len(fact_columns))})
                ON CONFLICT (timestamp, city) DO UPDATE
                SET aqi_us = EXCLUDED.aqi_us,
                    main_pollutant_us = EXCLUDED.main_pollutant_us,
                    aqi_cn = EXCLUDED.aqi_cn,
                    main_pollutant_cn = EXCLUDED.main_pollutant_cn,
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_dir = EXCLUDED.wind_dir;
            """
            cur.execute(insert_sql, fact_values)

        conn.commit()
        logger.info("AirVisual data successfully loaded into PostgreSQL.")
    except Exception as e:
        conn.rollback()
        logger.error("Error loading data into PostgreSQL: %s", e)
        raise
    finally:
        conn.close()


with DAG(
    dag_id="airvisual_etl",
    default_args=default_args,
    description="ETL pipeline for AirVisual API data into PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airquality", "etl"],
) as dag:
    t_get_data = PythonOperator(
        task_id="get_airvisual_data_hourly",
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    t_read_data = PythonOperator(
        task_id="read_data_airvisual",
        python_callable=read_data_airvisual,
    )

    t_load_data = PythonOperator(
        task_id="load_airvisual_to_postgresql",
        python_callable=load_airvisual_to_postgresql,
    )

    t_get_data >> t_read_data >> t_load_data