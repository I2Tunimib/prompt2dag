import os
import json
import shutil
import logging
import tempfile
import configparser
from datetime import datetime, timedelta

import requests
import pytz
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# ----------------------------------------------------------------------
# Helper functions for each task
# ----------------------------------------------------------------------


def get_airvisual_data_hourly(**context):
    """
    Fetch current air quality data from AirVisual API,
    validate response, convert timestamp to Bangkok timezone,
    deduplicate by checking PostgreSQL, and store JSON atomically.
    """
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.conf")
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    api_key = cfg.get("airvisual", "api_key")
    latitude = cfg.getfloat("airvisual", "latitude")
    longitude = cfg.getfloat("airvisual", "longitude")

    # Call AirVisual API
    url = (
        f"https://api.airvisual.com/v2/nearest_city?"
        f"lat={latitude}&lon={longitude}&key={api_key}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    # Basic validation
    if payload.get("status") != "success":
        raise ValueError("AirVisual API returned an unsuccessful status")

    # Extract and convert timestamp
    ts_str = payload["data"]["current"]["pollution"]["ts"]  # e.g., "2023-09-01T12:00:00.000Z"
    utc_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    local_dt = utc_dt.astimezone(bangkok_tz)
    hour_dt = local_dt.replace(minute=0, second=0, microsecond=0)

    # Deduplication check
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    sql = "SELECT 1 FROM factairvisualtable WHERE timestamp = %s LIMIT 1"
    exists = pg_hook.get_records(sql, parameters=(hour_dt,))
    if exists:
        raise AirflowSkipException(f"Data for {hour_dt} already exists in the database")

    # Attach extracted timestamp for downstream tasks
    payload["extracted_timestamp"] = hour_dt.isoformat()

    # Atomic write to local JSON file
    output_dir = "/tmp/airvisual"
    os.makedirs(output_dir, exist_ok=True)
    temp_fd, temp_path = tempfile.mkstemp(suffix=".json", dir=output_dir)
    with os.fdopen(temp_fd, "w") as tmp_file:
        json.dump(payload, tmp_file)
    final_path = os.path.join(output_dir, "airvisual_data.json")
    shutil.move(temp_path, final_path)

    # Push file path via XCom
    context["ti"].xcom_push(key="airvisual_path", value=final_path)


def read_data_airvisual(**context):
    """
    Read the JSON file produced by the previous task,
    validate its structure, and push the data via XCom.
    """
    ti = context["ti"]
    json_path = ti.xcom_pull(key="airvisual_path", task_ids="get_airvisual_data_hourly")
    if not json_path or not os.path.isfile(json_path):
        raise FileNotFoundError(f"AirVisual JSON file not found at {json_path}")

    with open(json_path, "r") as f:
        data = json.load(f)

    if "data" not in data:
        raise ValueError("Invalid AirVisual payload structure")

    logging.info("AirVisual data successfully read and validated")
    ti.xcom_push(key="airvisual_data", value=data)


def load_airvisual_to_postgresql(**context):
    """
    Load validated AirVisual data into PostgreSQL dimension and fact tables,
    using mapping files for pollution and weather codes.
    """
    ti = context["ti"]
    data = ti.xcom_pull(key="airvisual_data", task_ids="read_data_airvisual")
    if not data:
        raise ValueError("No AirVisual data found from previous task")

    # Load mapping configurations
    mapping_dir = os.path.join(os.path.dirname(__file__), "mappings")
    pollution_map_path = os.path.join(mapping_dir, "pollution_mapping.json")
    weather_map_path = os.path.join(mapping_dir, "weather_mapping.json")

    with open(pollution_map_path, "r") as f:
        pollution_map = json.load(f)
    with open(weather_map_path, "r") as f:
        weather_map = json.load(f)

    # Extract fields
    timestamp = data["extracted_timestamp"]
    city = data["data"]["city"]
    country = data["data"]["country"]
    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    # Map codes to descriptive values
    pollution_type = pollution_map.get(str(pollution.get("aqius")), "unknown")
    weather_desc = weather_map.get(weather.get("ic", ""), "unknown")

    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            # dimDateTimeTable
            cur.execute(
                """
                INSERT INTO dimDateTimeTable (timestamp)
                VALUES (%s)
                ON CONFLICT (timestamp) DO UPDATE SET timestamp = EXCLUDED.timestamp
                """,
                (timestamp,),
            )

            # dimLocationTable
            cur.execute(
                """
                INSERT INTO dimLocationTable (city, country)
                VALUES (%s, %s)
                ON CONFLICT (city, country) DO UPDATE SET city = EXCLUDED.city
                """,
                (city, country),
            )

            # dimMainPollutionTable
            cur.execute(
                """
                INSERT INTO dimMainPollutionTable (pollution_type)
                VALUES (%s)
                ON CONFLICT (pollution_type) DO NOTHING
                """,
                (pollution_type,),
            )

            # factairvisualtable
            cur.execute(
                """
                INSERT INTO factairvisualtable
                (timestamp, city, country, aqi_us, aqi_cn, weather_icon, weather_desc)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO UPDATE
                SET aqi_us = EXCLUDED.aqi_us,
                    aqi_cn = EXCLUDED.aqi_cn,
                    weather_icon = EXCLUDED.weather_icon,
                    weather_desc = EXCLUDED.weather_desc
                """,
                (
                    timestamp,
                    city,
                    country,
                    pollution.get("aqius"),
                    pollution.get("aqicn"),
                    weather.get("ic"),
                    weather_desc,
                ),
            )
        conn.commit()
        logging.info("AirVisual data loaded into PostgreSQL successfully")
    except Exception as exc:
        conn.rollback()
        logging.error("Failed to load AirVisual data into PostgreSQL: %s", exc)
        raise


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="airvisual_etl",
    default_args=default_args,
    description="ETL pipeline for AirVisual data into PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airquality", "etl"],
) as dag:
    get_airvisual_data_hourly = PythonOperator(
        task_id="get_airvisual_data_hourly",
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    read_data_airvisual = PythonOperator(
        task_id="read_data_airvisual",
        python_callable=read_data_airvisual,
    )

    load_airvisual_to_postgresql = PythonOperator(
        task_id="load_airvisual_to_postgresql",
        python_callable=load_airvisual_to_postgresql,
    )

    get_airvisual_data_hourly >> read_data_airvisual >> load_airvisual_to_postgresql