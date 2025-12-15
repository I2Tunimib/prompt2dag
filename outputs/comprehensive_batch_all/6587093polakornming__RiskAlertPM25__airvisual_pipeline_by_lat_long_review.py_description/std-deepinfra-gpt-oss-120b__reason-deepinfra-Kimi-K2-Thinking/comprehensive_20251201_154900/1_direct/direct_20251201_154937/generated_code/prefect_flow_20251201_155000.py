import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import configparser
import psycopg2
import requests
from prefect import flow, task, get_run_logger
from prefect.exceptions import Skip


CONFIG_PATH = Path("config.conf")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
JSON_FILE = DATA_DIR / "airvisual_hourly.json"
POLLUTION_MAPPING_FILE = DATA_DIR / "pollution_mapping.json"
WEATHER_MAPPING_FILE = DATA_DIR / "weather_mapping.json"


def _load_config() -> dict:
    """Load configuration values from ``config.conf``."""
    parser = configparser.ConfigParser()
    parser.read(CONFIG_PATH)
    cfg = {}
    if "airvisual" in parser:
        cfg.update(parser["airvisual"])
    if "postgres" in parser:
        cfg.update(parser["postgres"])
    return cfg


@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly() -> dict:
    """
    Fetch current air quality data from the AirVisual API,
    validate the response, convert timestamps to Bangkok timezone,
    and store the JSON payload atomically.

    Raises:
        Skip: If data for the current hour already exists in the database.
    """
    logger = get_run_logger()
    cfg = _load_config()
    api_key = cfg.get("api_key")
    lat = cfg.get("latitude", "13.8235")
    lon = cfg.get("longitude", "100.2745")
    if not api_key:
        raise ValueError("AirVisual API key not found in configuration.")

    url = (
        f"https://api.airvisual.com/v2/nearest_city?"
        f"lat={lat}&lon={lon}&key={api_key}"
    )
    logger.info("Requesting AirVisual data from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if payload.get("status") != "success":
        raise RuntimeError(f"AirVisual API error: {payload.get('data')}")

    # Convert timestamp to Bangkok timezone
    utc_ts = datetime.utcfromtimestamp(payload["data"]["current"]["pollution"]["ts"])
    bangkok_ts = utc_ts.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo("Asia/Bangkok")
    )
    payload["data"]["current"]["pollution"]["ts"] = bangkok_ts.isoformat()

    # Check for duplicate hour in PostgreSQL
    conn = psycopg2.connect(
        dbname=cfg.get("dbname"),
        user=cfg.get("user"),
        password=cfg.get("password"),
        host=cfg.get("host"),
        port=cfg.get("port", 5432),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM factairvisualtable
                WHERE timestamp = %s
                LIMIT 1
                """,
                (bangkok_ts,),
            )
            if cur.fetchone():
                logger.info(
                    "Data for %s already exists in the database. Skipping downstream tasks.",
                    bangkok_ts,
                )
                raise Skip("Duplicate hour")
    finally:
        conn.close()

    # Atomic write to JSON file
    logger.info("Saving API response to %s", JSON_FILE)
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, dir=DATA_DIR, suffix=".tmp"
    ) as tmp_file:
        json.dump(payload, tmp_file, ensure_ascii=False, indent=2)
        temp_path = Path(tmp_file.name)
    os.replace(temp_path, JSON_FILE)

    return payload


@task
def read_data_airvisual() -> dict:
    """
    Read the JSON file saved by ``get_airvisual_data_hourly`` and validate its structure.
    """
    logger = get_run_logger()
    if not JSON_FILE.is_file():
        raise FileNotFoundError(f"Expected JSON file not found: {JSON_FILE}")

    with JSON_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Basic validation
    if "data" not in data or "current" not in data["data"]:
        raise ValueError("Invalid AirVisual JSON structure.")
    logger.info("Successfully read and validated AirVisual data.")
    return data


@task
def load_airvisual_to_postgresql(airvisual_data: dict) -> None:
    """
    Load AirVisual data into PostgreSQL tables with conflict handling and transaction management.
    """
    logger = get_run_logger()
    cfg = _load_config()

    # Load mapping configurations
    def _load_mapping(path: Path) -> dict:
        if not path.is_file():
            logger.warning("Mapping file not found: %s", path)
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    pollution_mapping = _load_mapping(POLLUTION_MAPPING_FILE)
    weather_mapping = _load_mapping(WEATHER_MAPPING_FILE)

    conn = psycopg2.connect(
        dbname=cfg.get("dbname"),
        user=cfg.get("user"),
        password=cfg.get("password"),
        host=cfg.get("host"),
        port=cfg.get("port", 5432),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # Extract needed fields
                current = airvisual_data["data"]["current"]
                pollution = current["pollution"]
                weather = current["weather"]
                ts = datetime.fromisoformat(pollution["ts"])

                # Insert into dimDateTimeTable
                cur.execute(
                    """
                    INSERT INTO dimDateTimeTable (timestamp)
                    VALUES (%s)
                    ON CONFLICT (timestamp) DO NOTHING
                    """,
                    (ts,),
                )

                # Insert into dimLocationTable (using static coordinates for Salaya)
                cur.execute(
                    """
                    INSERT INTO dimLocationTable (latitude, longitude, city, country)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (latitude, longitude) DO NOTHING
                    """,
                    (
                        cfg.get("latitude", "13.8235"),
                        cfg.get("longitude", "100.2745"),
                        airvisual_data["data"]["city"],
                        airvisual_data["data"]["country"],
                    ),
                )

                # Insert into dimMainPollutionTable
                pollution_type = pollution_mapping.get(
                    str(pollution.get("aqius")), "unknown"
                )
                cur.execute(
                    """
                    INSERT INTO dimMainPollutionTable (pollution_type)
                    VALUES (%s)
                    ON CONFLICT (pollution_type) DO NOTHING
                    """,
                    (pollution_type,),
                )

                # Insert into factairvisualtable
                cur.execute(
                    """
                    INSERT INTO factairvisualtable (
                        timestamp,
                        latitude,
                        longitude,
                        city,
                        country,
                        aqi_us,
                        main_pollution,
                        temperature,
                        humidity,
                        wind_speed
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO UPDATE SET
                        aqi_us = EXCLUDED.aqi_us,
                        main_pollution = EXCLUDED.main_pollution,
                        temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        wind_speed = EXCLUDED.wind_speed
                    """,
                    (
                        ts,
                        cfg.get("latitude", "13.8235"),
                        cfg.get("longitude", "100.2745"),
                        airvisual_data["data"]["city"],
                        airvisual_data["data"]["country"],
                        pollution.get("aqius"),
                        pollution_type,
                        weather.get("tp"),
                        weather.get("hu"),
                        weather.get("ws"),
                    ),
                )
        logger.info("AirVisual data successfully loaded into PostgreSQL.")
    finally:
        conn.close()


@flow(name="airvisual_etl_flow")
def airvisual_etl_flow() -> None:
    """
    Orchestrates the AirVisual ETL pipeline:
    get_airvisual_data_hourly → read_data_airvisual → load_airvisual_to_postgresql
    """
    data = get_airvisual_data_hourly()
    validated = read_data_airvisual()
    load_airvisual_to_postgresql(validated)


if __name__ == "__main__":
    # Manual trigger; schedule configuration can be added via Prefect deployment UI.
    airvisual_etl_flow()