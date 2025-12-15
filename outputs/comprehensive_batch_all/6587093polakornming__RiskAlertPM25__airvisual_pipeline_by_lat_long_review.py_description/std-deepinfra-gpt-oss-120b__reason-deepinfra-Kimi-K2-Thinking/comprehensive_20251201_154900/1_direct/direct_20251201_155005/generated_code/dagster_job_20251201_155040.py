import datetime
import json
import os
import shutil
import tempfile
from typing import Dict

import pytz
import requests
from dagster import (
    In,
    Out,
    ResourceDefinition,
    RetryPolicy,
    SkipReason,
    job,
    op,
)


def postgres_resource(init_context):
    """Create a PostgreSQL connection using psycopg2.

    This is a minimal stub; replace with proper connection handling as needed.
    """
    import psycopg2

    config = init_context.resource_config
    conn = psycopg2.connect(
        dbname=config.get("dbname", "mydb"),
        user=config.get("user", "myuser"),
        password=config.get("password", "mypassword"),
        host=config.get("host", "localhost"),
        port=config.get("port", 5432),
    )
    conn.autocommit = False
    return conn


postgres_resource_def = ResourceDefinition(
    resource_fn=postgres_resource,
    config_schema={
        "dbname": str,
        "user": str,
        "password": str,
        "host": str,
        "port": int,
    },
)


@op(
    config_schema={
        "api_key": str,
        "latitude": float,
        "longitude": float,
    },
    out=Out(str),
    required_resource_keys={"postgres_resource"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Fetches current air quality data from AirVisual API.",
)
def get_airvisual_data_hourly(context) -> str:
    cfg = context.op_config
    api_key = cfg["api_key"]
    lat = cfg["latitude"]
    lon = cfg["longitude"]
    url = (
        f"https://api.airvisual.com/v2/nearest_city?"
        f"lat={lat}&lon={lon}&key={api_key}"
    )
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")

    payload = response.json()
    if "data" not in payload:
        raise Exception("Invalid API response: missing 'data' field")

    # Convert timestamp to Bangkok timezone
    utc_ts_str = payload["data"]["current"]["pollution"]["ts"]
    utc_dt = datetime.datetime.fromisoformat(utc_ts_str.replace("Z", "+00:00"))
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    bangkok_dt = utc_dt.astimezone(bangkok_tz)
    payload["data"]["current"]["pollution"]["ts_bangkok"] = bangkok_dt.isoformat()

    # Duplicate check in PostgreSQL
    conn = context.resources.postgres_resource
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM factairvisualtable WHERE ts = %s LIMIT 1",
            (bangkok_dt,),
        )
        if cur.fetchone():
            context.log.info("Data for this hour already exists. Skipping downstream.")
            raise SkipReason("Duplicate data")

    # Atomic write to JSON file
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, "airvisual_data.json")
    final_path = os.path.join(os.getcwd(), "airvisual_data.json")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    shutil.move(temp_path, final_path)
    shutil.rmtree(temp_dir)

    context.log.info(f"Saved AirVisual data to {final_path}")
    return final_path


@op(
    ins={"json_path": In(str)},
    out=Out(dict),
    description="Reads the saved JSON file and validates its structure.",
)
def read_data_airvisual(context, json_path: str) -> Dict:
    if not os.path.exists(json_path):
        raise Exception(f"JSON file not found at {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "data" not in data:
        raise Exception("Invalid JSON structure: missing 'data' field")

    context.log.info("Successfully read and validated AirVisual data.")
    return data


@op(
    required_resource_keys={"postgres_resource"},
    ins={"airvisual_data": In(dict)},
    description="Loads AirVisual data into PostgreSQL with conflict handling.",
)
def load_airvisual_to_postgresql(context, airvisual_data: Dict) -> None:
    # Load mapping configurations (stubbed – adjust paths as needed)
    pollution_map_path = os.path.join(os.getcwd(), "pollution_mapping.json")
    weather_map_path = os.path.join(os.getcwd(), "weather_mapping.json")
    if os.path.exists(pollution_map_path):
        with open(pollution_map_path, "r", encoding="utf-8") as f:
            pollution_mapping = json.load(f)
    else:
        pollution_mapping = {}
    if os.path.exists(weather_map_path):
        with open(weather_map_path, "r", encoding="utf-8") as f:
            weather_mapping = json.load(f)
    else:
        weather_mapping = {}

    conn = context.resources.postgres_resource
    try:
        with conn:
            with conn.cursor() as cur:
                # dimDateTimeTable
                ts_str = airvisual_data["data"]["current"]["pollution"]["ts_bangkok"]
                ts = datetime.datetime.fromisoformat(ts_str)
                cur.execute(
                    """
                    INSERT INTO dimDateTimeTable (ts)
                    VALUES (%s)
                    ON CONFLICT (ts) DO NOTHING
                    """,
                    (ts,),
                )

                # dimLocationTable
                city = airvisual_data["data"]["city"]
                country = airvisual_data["data"]["country"]
                cur.execute(
                    """
                    INSERT INTO dimLocationTable (city, country)
                    VALUES (%s, %s)
                    ON CONFLICT (city, country) DO NOTHING
                    """,
                    (city, country),
                )

                # dimMainPollutionTable
                pollution = airvisual_data["data"]["current"]["pollution"]
                cur.execute(
                    """
                    INSERT INTO dimMainPollutionTable (aqius, mainus)
                    VALUES (%s, %s)
                    ON CONFLICT (aqius, mainus) DO NOTHING
                    """,
                    (pollution["aqius"], pollution["mainus"]),
                )

                # factairvisualtable
                weather = airvisual_data["data"]["current"]["weather"]
                cur.execute(
                    """
                    INSERT INTO factairvisualtable
                    (ts, city, country, aqius, mainus, temperature, humidity, pressure)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        pressure = EXCLUDED.pressure
                    """,
                    (
                        ts,
                        city,
                        country,
                        pollution["aqius"],
                        pollution["mainus"],
                        weather["tp"],
                        weather["hu"],
                        weather["pr"],
                    ),
                )
        context.log.info("AirVisual data loaded into PostgreSQL successfully.")
    except Exception as exc:
        conn.rollback()
        context.log.error(f"Failed to load data into PostgreSQL: {exc}")
        raise


@job(
    resource_defs={"postgres_resource": postgres_resource_def},
    description="ETL pipeline for AirVisual data.",
)
def airvisual_etl_job():
    json_path = get_airvisual_data_hourly()
    data = read_data_airvisual(json_path)
    load_airvisual_to_postgresql(data)


if __name__ == "__main__":
    result = airvisual_etl_job.execute_in_process(
        run_config={
            "ops": {
                "get_airvisual_data_hourly": {
                    "config": {
                        "api_key": "YOUR_API_KEY",
                        "latitude": 13.7563,
                        "longitude": 100.5018,
                    }
                }
            },
            "resources": {
                "postgres_resource": {
                    "config": {
                        "dbname": "mydb",
                        "user": "myuser",
                        "password": "mypassword",
                        "host": "localhost",
                        "port": 5432,
                    }
                }
            },
        }
    )
    assert result.success, "Job execution failed"