import os
import json
import logging
import tempfile
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import psycopg2
import psycopg2.extras
from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip

# ----------------------------------------------------------------------
# Configuration helpers
# ----------------------------------------------------------------------


def _load_config(config_path: str = "config.conf") -> dict:
    """Load simple key=value configuration file."""
    config = {}
    if not os.path.exists(config_path):
        return config
    with open(config_path, "r", encoding="utf-8") as cfg:
        for line in cfg:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
    return config


CONFIG = _load_config()
API_KEY = CONFIG.get("AIRVISUAL_API_KEY", "")
LATITUDE = CONFIG.get("AIRVISUAL_LATITUDE", "13.8235")
LONGITUDE = CONFIG.get("AIRVISUAL_LONGITUDE", "100.2740")
TIMEZONE = ZoneInfo("Asia/Bangkok")
JSON_TEMP_DIR = CONFIG.get("JSON_TEMP_DIR", "./temp")
POLLUTION_MAP_PATH = CONFIG.get("POLLUTION_MAP_PATH", "pollution_mapping.json")
WEATHER_MAP_PATH = CONFIG.get("WEATHER_MAP_PATH", "weather_mapping.json")
POSTGRES_DSN = os.getenv("POSTGRES_CONN", CONFIG.get("POSTGRES_CONN", ""))


# ----------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly() -> str:
    """
    Fetch current air quality data from AirVisual API, validate, convert timestamp,
    check for duplicates, and write JSON atomically to a temporary file.

    Returns
    -------
    str
        Path to the saved JSON file.
    """
    logger = get_run_logger()
    if not API_KEY:
        raise RuntimeError("AirVisual API key not configured.")

    url = (
        f"https://api.airvisual.com/v2/nearest_city?"
        f"lat={LATITUDE}&lon={LONGITUDE}&key={API_KEY}"
    )
    logger.info("Requesting AirVisual data from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    # Basic validation
    if payload.get("status") != "success":
        raise RuntimeError(f"AirVisual API error: {payload.get('data')}")

    data = payload["data"]
    ts_utc = datetime.utcfromtimestamp(data["current"]["pollution"]["ts"])
    ts_local = ts_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(TIMEZONE)
    data["timestamp_local"] = ts_local.isoformat()

    # Duplicate check
    if _record_exists(ts_local):
        logger.info("Record for %s already exists. Skipping downstream tasks.", ts_local)
        raise Skip(f"Data for hour {ts_local} already processed.")

    # Ensure temp directory exists
    os.makedirs(JSON_TEMP_DIR, exist_ok=True)
    temp_fd, temp_path = tempfile.mkstemp(suffix=".json", dir=JSON_TEMP_DIR)
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as tmp_file:
            json.dump(data, tmp_file, ensure_ascii=False, indent=2)
        final_path = os.path.join(JSON_TEMP_DIR, f"airvisual_{ts_local:%Y%m%d%H}.json")
        shutil.move(temp_path, final_path)
        logger.info("Saved AirVisual data to %s", final_path)
        return final_path
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _record_exists(timestamp: datetime) -> bool:
    """Check if a record for the given hour already exists in the fact table."""
    if not POSTGRES_DSN:
        raise RuntimeError("PostgreSQL DSN not configured.")
    hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
    query = """
        SELECT 1 FROM factairvisualtable
        WHERE timestamp = %s
        LIMIT 1;
    """
    try:
        with psycopg2.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (hour_start,))
                return cur.fetchone() is not None
    except Exception as exc:
        raise RuntimeError(f"Failed to check duplicate record: {exc}") from exc


@task
def read_data_airvisual(json_path: str) -> dict:
    """
    Read the saved JSON file and validate its structure.

    Parameters
    ----------
    json_path : str
        Path to the JSON file created by `get_airvisual_data_hourly`.

    Returns
    -------
    dict
        Parsed JSON data.
    """
    logger = get_run_logger()
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Minimal validation
    required_keys = {"timestamp_local", "current"}
    if not required_keys.issubset(data):
        raise ValueError("Invalid AirVisual data structure.")

    logger.info("Successfully read AirVisual data from %s", json_path)
    return data


@task
def load_airvisual_to_postgresql(data: dict) -> None:
    """
    Load AirVisual data into PostgreSQL with dimension and fact tables,
    handling conflicts and ensuring transaction integrity.

    Parameters
    ----------
    data : dict
        Parsed AirVisual data.
    """
    logger = get_run_logger()
    if not POSTGRES_DSN:
        raise RuntimeError("PostgreSQL DSN not configured.")

    # Load mapping configurations
    with open(POLLUTION_MAP_PATH, "r", encoding="utf-8") as f:
        pollution_map = json.load(f)
    with open(WEATHER_MAP_PATH, "r", encoding="utf-8") as f:
        weather_map = json.load(f)

    timestamp = datetime.fromisoformat(data["timestamp_local"])
    location = data.get("city", "Unknown")
    country = data.get("country", "Unknown")
    state = data.get("state", "Unknown")
    lat = data.get("location", {}).get("coordinates", [None, None])[0]
    lon = data.get("location", {}).get("coordinates", [None, None])[1]

    pollution = data["current"]["pollution"]
    weather = data["current"]["weather"]

    # Prepare insert statements with conflict handling
    insert_dim_datetime = """
        INSERT INTO dimDateTimeTable (timestamp)
        VALUES (%s)
        ON CONFLICT (timestamp) DO NOTHING;
    """
    insert_dim_location = """
        INSERT INTO dimLocationTable (city, state, country, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (city, state, country) DO UPDATE
        SET latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
    """
    insert_dim_pollution = """
        INSERT INTO dimMainPollutionTable (pollution_type, description)
        VALUES (%s, %s)
        ON CONFLICT (pollution_type) DO NOTHING;
    """
    insert_fact = """
        INSERT INTO factairvisualtable (
            timestamp,
            city,
            state,
            country,
            latitude,
            longitude,
            aqi,
            main_pollutant,
            temperature,
            humidity,
            pressure,
            wind_speed,
            wind_dir
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (timestamp) DO UPDATE
        SET aqi = EXCLUDED.aqi,
            main_pollutant = EXCLUDED.main_pollutant,
            temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity,
            pressure = EXCLUDED.pressure,
            wind_speed = EXCLUDED.wind_speed,
            wind_dir = EXCLUDED.wind_dir;
    """

    try:
        with psycopg2.connect(POSTGRES_DSN) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # DimDateTime
                cur.execute(insert_dim_datetime, (timestamp,))
                # DimLocation
                cur.execute(
                    insert_dim_location,
                    (location, state, country, lat, lon),
                )
                # DimPollution (using mapping)
                main_pollutant_code = pollution.get("aqius")
                main_pollutant_desc = pollution_map.get(str(main_pollutant_code), "Unknown")
                cur.execute(
                    insert_dim_pollution,
                    (main_pollutant_code, main_pollutant_desc),
                )
                # Fact table
                cur.execute(
                    insert_fact,
                    (
                        timestamp,
                        location,
                        state,
                        country,
                        lat,
                        lon,
                        pollution.get("aqius"),
                        main_pollutant_code,
                        weather.get("tp"),
                        weather.get("hu"),
                        weather.get("pr"),
                        weather.get("ws"),
                        weather.get("wd"),
                    ),
                )
            conn.commit()
        logger.info("AirVisual data loaded successfully into PostgreSQL.")
    except Exception as exc:
        logger.error("Failed to load data into PostgreSQL: %s", exc)
        raise


# ----------------------------------------------------------------------
# Flow orchestration
# ----------------------------------------------------------------------


@flow
def airvisual_etl_flow() -> None:
    """
    Orchestrates the AirVisual ETL pipeline:
    1. Fetch data from API.
    2. Read and validate saved JSON.
    3. Load data into PostgreSQL.
    """
    json_path = get_airvisual_data_hourly()
    data = read_data_airvisual(json_path)
    load_airvisual_to_postgresql(data)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------


if __name__ == "__main__":
    # Manual trigger; scheduling is handled via Prefect deployment configuration.
    airvisual_etl_flow()