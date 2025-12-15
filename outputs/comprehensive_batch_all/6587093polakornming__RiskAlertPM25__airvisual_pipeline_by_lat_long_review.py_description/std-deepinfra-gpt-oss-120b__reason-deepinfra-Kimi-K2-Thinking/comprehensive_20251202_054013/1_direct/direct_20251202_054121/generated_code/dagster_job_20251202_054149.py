from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict

import requests
from dagster import (
    Config,
    ConfigurableResource,
    InitResourceContext,
    In,
    job,
    op,
    Out,
    RetryPolicy,
    SkipReason,
    op,
    job,
)
from dagster import resource
from dagster import OpExecutionContext

# ----------------------------------------------------------------------
# Resources
# ----------------------------------------------------------------------


class PostgresResource(ConfigurableResource):
    """Minimal PostgreSQL resource using psycopg2.

    In a real deployment, replace with a robust connection pool or SQLAlchemy engine.
    """

    conn_str: str

    def get_connection(self):
        import psycopg2

        return psycopg2.connect(self.conn_str)


# ----------------------------------------------------------------------
# Config schemas
# ----------------------------------------------------------------------


class AirvisualConfig(Config):
    """Configuration for the AirVisual API request."""

    api_key: str
    latitude: float
    longitude: float
    # Directory where the JSON payload will be stored
    output_dir: str = "./data"


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------


def _bangkok_now() -> datetime:
    """Return current time in Bangkok timezone (UTC+7)."""
    utc_now = datetime.now(timezone.utc)
    bangkok_offset = timedelta(hours=7)
    return utc_now + bangkok_offset


def _write_atomic(json_obj: Dict[str, Any], target_path: Path) -> None:
    """Write JSON to a temporary file and atomically move to target_path."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=target_path.parent) as tmp_file:
        json.dump(json_obj, tmp_file, ensure_ascii=False, indent=2)
        temp_name = tmp_file.name
    shutil.move(temp_name, target_path)


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"postgres"},
    out={"file_path": Out(str)},
    config_schema=AirvisualConfig,
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> str:
    """Fetch current air quality data from AirVisual API, deduplicate, and store as JSON."""
    cfg: AirvisualConfig = context.op_config
    url = (
        "https://api.airvisual.com/v2/nearest_city?"
        f"lat={cfg.latitude}&lon={cfg.longitude}&key={cfg.api_key}"
    )
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise Exception(f"AirVisual API request failed with status {response.status_code}")

    payload = response.json()
    if "data" not in payload:
        raise Exception("Invalid API response: missing 'data' field")

    # Convert timestamp to Bangkok timezone
    utc_timestamp_str = payload["data"]["current"]["pollution"]["ts"]
    utc_dt = datetime.fromisoformat(utc_timestamp_str.replace("Z", "+00:00"))
    bangkok_dt = utc_dt.astimezone(timezone(timedelta(hours=7)))
    payload["data"]["current"]["pollution"]["ts_bangkok"] = bangkok_dt.isoformat()

    # Check for existing record for the same hour
    conn = context.resources.postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM factairvisualtable
                WHERE date_trunc('hour', timestamp) = date_trunc('hour', %s)
                LIMIT 1;
                """,
                (bangkok_dt,),
            )
            exists = cur.fetchone() is not None
    finally:
        conn.close()

    if exists:
        raise SkipReason("Data for the current hour already exists in the database.")

    # Write JSON atomically
    timestamp_str = bangkok_dt.strftime("%Y%m%d%H%M%S")
    filename = f"airvisual_{timestamp_str}.json"
    output_path = Path(cfg.output_dir) / filename
    _write_atomic(payload, output_path)

    context.log.info(f"AirVisual data written to {output_path}")
    return str(output_path)


@op(
    out={"data": Out(dict)},
)
def read_data_airvisual(context: OpExecutionContext, file_path: str) -> Dict[str, Any]:
    """Read the JSON file produced by the extraction step and validate its structure."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected JSON file not found at {file_path}")

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    # Minimal validation: ensure required keys exist
    required_keys = {"data", "status"}
    if not required_keys.issubset(payload.keys()):
        raise ValueError("JSON payload missing required top-level keys")

    context.log.info(f"Successfully read and validated data from {file_path}")
    return payload


@op(
    required_resource_keys={"postgres"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def load_airvisual_to_postgresql(context: OpExecutionContext, data: Dict[str, Any]) -> None:
    """Load AirVisual data into PostgreSQL with conflict handling and transaction management."""
    # Load mapping configurations (stubbed with empty dicts if files missing)
    def load_mapping(file_name: str) -> Dict[str, Any]:
        path = Path(file_name)
        if path.is_file():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    pollution_mapping = load_mapping("pollution_mapping.json")
    weather_mapping = load_mapping("weather_mapping.json")

    # Extract relevant fields
    current = data["data"]["current"]
    pollution = current["pollution"]
    weather = current["weather"]
    location = data["data"]["city"]
    timestamp = datetime.fromisoformat(pollution["ts_bangkok"])

    conn = context.resources.postgres.get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Insert into dimDateTimeTable
                cur.execute(
                    """
                    INSERT INTO dimDateTimeTable (timestamp)
                    VALUES (%s)
                    ON CONFLICT (timestamp) DO NOTHING;
                    """,
                    (timestamp,),
                )

                # Insert into dimLocationTable
                cur.execute(
                    """
                    INSERT INTO dimLocationTable (city, country, latitude, longitude)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (city, country) DO NOTHING;
                    """,
                    (
                        location,
                        data["data"]["country"],
                        cfg.latitude,
                        cfg.longitude,
                    ),
                )

                # Insert into dimMainPollutionTable
                cur.execute(
                    """
                    INSERT INTO dimMainPollutionTable (aqi, mainus)
                    VALUES (%s, %s)
                    ON CONFLICT (aqi, mainus) DO NOTHING;
                    """,
                    (pollution["aqius"], pollution["mainus"]),
                )

                # Insert into factairvisualtable
                cur.execute(
                    """
                    INSERT INTO factairvisualtable (
                        timestamp,
                        city,
                        country,
                        latitude,
                        longitude,
                        aqi_us,
                        main_pollutant_us,
                        temperature,
                        humidity,
                        wind_speed
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, city) DO UPDATE SET
                        aqi_us = EXCLUDED.aqi_us,
                        main_pollutant_us = EXCLUDED.main_pollutant_us,
                        temperature = EXCLUDED.temperature,
                        humidity = EXCLUDED.humidity,
                        wind_speed = EXCLUDED.wind_speed;
                    """,
                    (
                        timestamp,
                        location,
                        data["data"]["country"],
                        cfg.latitude,
                        cfg.longitude,
                        pollution["aqius"],
                        pollution["mainus"],
                        weather["tp"],
                        weather["hu"],
                        weather["ws"],
                    ),
                )
        context.log.info("Data successfully loaded into PostgreSQL.")
    finally:
        conn.close()


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------


@job(
    resource_defs={"postgres": PostgresResource()},
    retry_policy=RetryPolicy(max_retries=3, delay=timedelta(minutes=3)),
)
def airvisual_etl_job():
    """ETL pipeline: extract → read → load."""
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)


# ----------------------------------------------------------------------
# Entry point for local execution
# ----------------------------------------------------------------------


if __name__ == "__main__":
    # Example configuration; replace with real values or load from a file.
    run_config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "api_key": "YOUR_API_KEY",
                    "latitude": 13.7563,
                    "longitude": 100.5018,
                    "output_dir": "./data",
                }
            }
        },
        "resources": {
            "postgres": {
                "config": {
                    "conn_str": "postgresql://user:password@localhost:5432/yourdb"
                }
            }
        },
    }

    result = airvisual_etl_job.execute_in_process(run_config=run_config)
    assert result.success