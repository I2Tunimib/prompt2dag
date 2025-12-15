import json
import logging
import os
import shutil
from datetime import datetime
from typing import Any, Dict

import pytz
import requests
from dagster import (
    Config,
    Field,
    OpExecutionContext,
    RetryPolicy,
    SkipReason,
    asset,
    job,
    op,
    resource,
)

logger = logging.getLogger(__name__)


class AirvisualConfig(Config):
    api_key: str = Field(
        description="AirVisual API key."
    )
    latitude: float = Field(
        default=13.7563,
        description="Latitude for the location.",
    )
    longitude: float = Field(
        default=100.5018,
        description="Longitude for the location.",
    )
    output_path: str = Field(
        default="airvisual_data.json",
        description="Path to store the fetched JSON data.",
    )


class ReadConfig(Config):
    input_path: str = Field(
        default="airvisual_data.json",
        description="Path of the JSON file to read.",
    )


class LoadConfig(Config):
    pollution_mapping_path: str = Field(
        default="pollution_mapping.json",
        description="Path to pollution mapping JSON.",
    )
    weather_mapping_path: str = Field(
        default="weather_mapping.json",
        description="Path to weather mapping JSON.",
    )


@resource
def postgres_resource(_):
    """
    Minimal stub for a PostgreSQL resource.
    Replace with a real connection (e.g., using psycopg2 or SQLAlchemy) in production.
    """
    class StubConnection:
        def execute(self, query: str, params: Any = None):
            logger.info("Executing query: %s", query)

        def commit(self):
            logger.info("Committing transaction.")

        def rollback(self):
            logger.info("Rolling back transaction.")

        def close(self):
            logger.info("Closing connection.")

    return StubConnection()


def _bangkok_now() -> datetime:
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    return utc_now.astimezone(bangkok_tz)


def _record_exists(conn, timestamp: datetime) -> bool:
    """
    Stub implementation that always returns False.
    Replace with a real SELECT query checking for existing records.
    """
    logger.info("Checking for existing record at %s", timestamp.isoformat())
    return False


@op(
    config_schema=AirvisualConfig,
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Fetches current air quality data from AirVisual API and stores it locally.",
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> None:
    cfg: AirvisualConfig = context.op_config
    url = (
        "https://api.airvisual.com/v2/nearest_city?"
        f"lat={cfg.latitude}&lon={cfg.longitude}&key={cfg.api_key}"
    )
    logger.info("Requesting AirVisual data from %s", url)
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        raise Exception(f"AirVisual API request failed with status {response.status_code}")

    data = response.json()
    if "data" not in data:
        raise Exception("Invalid API response structure: missing 'data' key")

    timestamp = _bangkok_now()
    conn = context.resources.postgres_resource
    if _record_exists(conn, timestamp):
        raise SkipReason("Data for the current hour already exists in the database.")

    # Prepare payload with timestamp
    payload = {
        "timestamp": timestamp.isoformat(),
        "data": data["data"],
    }

    temp_path = f"{cfg.output_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        shutil.move(temp_path, cfg.output_path)
        logger.info("AirVisual data written to %s", cfg.output_path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


@op(
    config_schema=ReadConfig,
    description="Reads the saved JSON file to verify data extraction success.",
)
def read_data_airvisual(context: OpExecutionContext) -> Dict[str, Any]:
    cfg: ReadConfig = context.op_config
    if not os.path.isfile(cfg.input_path):
        raise FileNotFoundError(f"Input file not found: {cfg.input_path}")

    with open(cfg.input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "timestamp" not in data or "data" not in data:
        raise ValueError("JSON structure is invalid; missing required keys.")

    logger.info("Successfully read AirVisual data from %s", cfg.input_path)
    return data


@op(
    config_schema=LoadConfig,
    required_resource_keys={"postgres_resource"},
    description="Loads AirVisual data into PostgreSQL with conflict resolution.",
)
def load_airvisual_to_postgresql(
    context: OpExecutionContext, airvisual_payload: Dict[str, Any]
) -> None:
    cfg: LoadConfig = context.op_config
    conn = context.resources.postgres_resource

    # Load mapping files (stubbed as empty dicts if files are missing)
    def load_mapping(path: str) -> Dict[str, Any]:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        logger.warning("Mapping file not found: %s. Using empty mapping.", path)
        return {}

    pollution_mapping = load_mapping(cfg.pollution_mapping_path)
    weather_mapping = load_mapping(cfg.weather_mapping_path)

    try:
        # Begin transaction (stubbed)
        logger.info("Starting transaction for AirVisual load.")
        timestamp = airvisual_payload["timestamp"]
        data = airvisual_payload["data"]

        # Insert into dimDateTimeTable (conflict resolution stub)
        conn.execute(
            "INSERT INTO dimDateTimeTable (timestamp) VALUES (%s) ON CONFLICT DO NOTHING;",
            (timestamp,),
        )

        # Insert into dimLocationTable (stubbed values)
        location = data.get("location", {})
        conn.execute(
            "INSERT INTO dimLocationTable (city, country, latitude, longitude) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (
                location.get("city"),
                location.get("country"),
                location.get("coordinates", {}).get("latitude"),
                location.get("coordinates", {}).get("longitude"),
            ),
        )

        # Insert pollution metadata into dimMainPollutionTable
        pollution = data.get("current", {}).get("pollution", {})
        conn.execute(
            "INSERT INTO dimMainPollutionTable (aqius, mainus) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING;",
            (pollution.get("aqius"), pollution.get("mainus")),
        )

        # Insert fact data into factairvisualtable
        weather = data.get("current", {}).get("weather", {})
        fact_values = {
            "timestamp": timestamp,
            "city": location.get("city"),
            "aqius": pollution.get("aqius"),
            "temperature": weather.get("tp"),
            "humidity": weather.get("hu"),
            "wind_speed": weather.get("ws"),
        }
        conn.execute(
            "INSERT INTO factairvisualtable (timestamp, city, aqius, temperature, humidity, wind_speed) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING;",
            (
                fact_values["timestamp"],
                fact_values["city"],
                fact_values["aqius"],
                fact_values["temperature"],
                fact_values["humidity"],
                fact_values["wind_speed"],
            ),
        )

        conn.commit()
        logger.info("AirVisual data successfully loaded into PostgreSQL.")
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load AirVisual data: %s", exc)
        raise


@job(
    description="ETL pipeline that extracts AirVisual data, validates it, and loads it into PostgreSQL.",
)
def airvisual_etl_job():
    data = get_airvisual_data_hourly()
    payload = read_data_airvisual()
    load_airvisual_to_postgresql(payload)


if __name__ == "__main__":
    result = airvisual_etl_job.execute_in_process()
    if result.success:
        logger.info("AirVisual ETL job completed successfully.")
    else:
        logger.error("AirVisual ETL job failed.")