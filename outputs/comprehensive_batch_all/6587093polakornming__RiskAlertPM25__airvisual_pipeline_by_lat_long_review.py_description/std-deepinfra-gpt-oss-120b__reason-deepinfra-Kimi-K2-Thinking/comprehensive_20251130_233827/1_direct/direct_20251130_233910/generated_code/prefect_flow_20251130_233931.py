import json
import os
import shutil
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip
from sqlalchemy import create_engine, text

# Configuration paths
CONFIG_PATH = "config.conf"
DATA_DIR = "data"
JSON_DATA_PATH = os.path.join(DATA_DIR, "airvisual_data.json")
POLLUTION_MAPPING_PATH = os.path.join(DATA_DIR, "pollution_mapping.json")
WEATHER_MAPPING_PATH = os.path.join(DATA_DIR, "weather_mapping.json")

# Database connection (adjust as needed)
POSTGRES_CONN_URL = "postgresql+psycopg2://user:password@localhost:5432/yourdb"
engine = create_engine(POSTGRES_CONN_URL)


def _load_config():
    """Load API key and coordinates from a simple INI config file."""
    import configparser

    parser = configparser.ConfigParser()
    parser.read(CONFIG_PATH)
    cfg = parser["airvisual"]
    return {
        "api_key": cfg.get("api_key"),
        "lat": cfg.getfloat("lat"),
        "lon": cfg.getfloat("lon"),
    }


@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly() -> str:
    """
    Fetch current air quality data from AirVisual API,
    validate response, convert timestamp to Bangkok timezone,
    check for duplicate records, and atomically write JSON to disk.

    Returns:
        Path to the saved JSON file.
    """
    logger = get_run_logger()
    cfg = _load_config()
    url = (
        "https://api.airvisual.com/v2/nearest_city"
        f"?lat={cfg['lat']}&lon={cfg['lon']}&key={cfg['api_key']}"
    )
    logger.info("Requesting AirVisual data from %s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Basic validation
    if "status" not in data or data["status"] != "success":
        raise ValueError("AirVisual API returned an unsuccessful status")

    # Extract and convert timestamp
    try:
        utc_ts_str = data["current"]["pollution"]["ts"]
        utc_dt = datetime.fromisoformat(utc_ts_str.replace("Z", "+00:00"))
        bangkok_dt = utc_dt.astimezone(ZoneInfo("Asia/Bangkok"))
        data["current"]["pollution"]["ts_bangkok"] = bangkok_dt.isoformat()
    except Exception as exc:
        raise ValueError(f"Failed to parse timestamp: {exc}")

    # Duplicate check (hourly granularity)
    hour_start = bangkok_dt.replace(minute=0, second=0, microsecond=0)
    query = text(
        "SELECT 1 FROM factairvisualtable WHERE timestamp = :ts LIMIT 1"
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"ts": hour_start})
        if result.first():
            logger.info("Data for %s already exists. Skipping.", hour_start)
            raise Skip(f"Record for hour {hour_start} already exists")

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Atomic write
    with tempfile.NamedTemporaryFile("w", delete=False, dir=DATA_DIR) as tmp_file:
        json.dump(data, tmp_file, ensure_ascii=False, indent=2)
        temp_path = tmp_file.name
    shutil.move(temp_path, JSON_DATA_PATH)
    logger.info("AirVisual data saved to %s", JSON_DATA_PATH)
    return JSON_DATA_PATH


@task
def read_data_airvisual(json_path: str) -> dict:
    """
    Read the saved JSON file and validate its structure.

    Args:
        json_path: Path to the JSON file produced by the previous task.

    Returns:
        Parsed JSON data as a dictionary.
    """
    logger = get_run_logger()
    if not os.path.isfile(json_path):
        raise FileNotFoundError(f"JSON file not found at {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Minimal structure validation
    if "current" not in data or "pollution" not in data["current"]:
        raise ValueError("Invalid AirVisual data structure")
    logger.info("AirVisual data successfully read and validated")
    return data


@task
def load_airvisual_to_postgresql(data: dict):
    """
    Load AirVisual data into PostgreSQL with conflict handling
    and transaction management.

    Args:
        data: Parsed AirVisual JSON payload.
    """
    logger = get_run_logger()

    # Load mapping configurations
    def _load_mapping(path):
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    pollution_map = _load_mapping(POLLUTION_MAPPING_PATH)
    weather_map = _load_mapping(WEATHER_MAPPING_PATH)

    # Extract fields
    pollution = data["current"]["pollution"]
    weather = data["current"]["weather"]
    ts_bangkok = datetime.fromisoformat(pollution["ts_bangkok"])

    location_vals = {
        "city": data.get("city", "Unknown"),
        "state": data.get("state", "Unknown"),
        "country": data.get("country", "Unknown"),
        "latitude": data.get("location", {}).get("coordinates", [None, None])[1],
        "longitude": data.get("location", {}).get("coordinates", [None, None])[0],
    }

    # Begin transaction
    with engine.begin() as conn:
        # dimDateTimeTable
        conn.execute(
            text(
                """
                INSERT INTO dimDateTimeTable (timestamp)
                VALUES (:ts)
                ON CONFLICT (timestamp) DO NOTHING
                """
            ),
            {"ts": ts_bangkok},
        )

        # dimLocationTable
        conn.execute(
            text(
                """
                INSERT INTO dimLocationTable (city, state, country, latitude, longitude)
                VALUES (:city, :state, :country, :latitude, :longitude)
                ON CONFLICT (city, state, country) DO UPDATE
                SET latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
                """
            ),
            location_vals,
        )

        # dimMainPollutionTable
        pollution_vals = {
            "aqi_us": pollution.get("aqius"),
            "aqi_cn": pollution.get("aqicn"),
            "main_pollutant": pollution.get("mainus"),
        }
        conn.execute(
            text(
                """
                INSERT INTO dimMainPollutionTable (aqi_us, aqi_cn, main_pollutant)
                VALUES (:aqi_us, :aqi_cn, :main_pollutant)
                ON CONFLICT (aqi_us, aqi_cn) DO NOTHING
                """
            ),
            pollution_vals,
        )

        # factairvisualtable
        fact_vals = {
            "timestamp": ts_bangkok,
            "city": location_vals["city"],
            "state": location_vals["state"],
            "country": location_vals["country"],
            "aqi_us": pollution_vals["aqi_us"],
            "aqi_cn": pollution_vals["aqi_cn"],
            "main_pollutant": pollution_vals["main_pollutant"],
            "temperature": weather.get("tp"),
            "humidity": weather.get("hu"),
            "wind_speed": weather.get("ws"),
        }
        conn.execute(
            text(
                """
                INSERT INTO factairvisualtable (
                    timestamp, city, state, country,
                    aqi_us, aqi_cn, main_pollutant,
                    temperature, humidity, wind_speed
                )
                VALUES (
                    :timestamp, :city, :state, :country,
                    :aqi_us, :aqi_cn, :main_pollutant,
                    :temperature, :humidity, :wind_speed
                )
                ON CONFLICT (timestamp) DO UPDATE
                SET aqi_us = EXCLUDED.aqi_us,
                    aqi_cn = EXCLUDED.aqi_cn,
                    main_pollutant = EXCLUDED.main_pollutant,
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    wind_speed = EXCLUDED.wind_speed
                """
            ),
            fact_vals,
        )
    logger.info("AirVisual data loaded into PostgreSQL successfully")


@flow(retries=3, retry_delay_seconds=180)
def airvisual_etl_flow():
    """
    Orchestrates the AirVisual ETL pipeline:
    get data → read & validate → load into PostgreSQL.
    """
    json_path = get_airvisual_data_hourly()
    data = read_data_airvisual(json_path)
    load_airvisual_to_postgresql(data)


if __name__ == "__main__":
    # Manual trigger; schedule configuration can be added via Prefect deployment UI
    airvisual_etl_flow()