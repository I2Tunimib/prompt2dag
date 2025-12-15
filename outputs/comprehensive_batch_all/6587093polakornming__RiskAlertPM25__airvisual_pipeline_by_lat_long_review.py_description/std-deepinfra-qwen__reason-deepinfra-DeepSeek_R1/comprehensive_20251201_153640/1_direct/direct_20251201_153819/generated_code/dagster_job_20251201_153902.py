from dagster import op, job, RetryPolicy, Failure, Field, String, resource
from datetime import datetime, timezone
import pytz
import requests
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from airflow.exceptions import AirflowSkipException


@resource(config_schema={"api_key": Field(String)})
def airvisual_api_resource(context):
    return context.resource_config["api_key"]


@resource(config_schema={"db_conn_id": Field(String)})
def postgres_resource(context):
    conn_info = context.resource_config["db_conn_id"]
    return psycopg2.connect(conn_info)


@op(required_resource_keys={"airvisual_api"})
def get_airvisual_data_hourly(context):
    api_key = context.resources.airvisual_api
    lat, lon = 13.7563, 100.3084  # Salaya, Nakhon Pathom, Thailand
    url = f"https://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if data["status"] != "success":
        raise Failure(f"API request failed: {data['data']}")

    data["data"]["time"] = pytz.timezone("Asia/Bangkok").localize(
        datetime.fromtimestamp(data["data"]["time"]["s"])
    ).isoformat()

    file_path = "airvisual_data.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # Check if data already exists in the database
    with context.resources.postgres_resource.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM factairvisualtable WHERE timestamp = %s",
            (data["data"]["time"],)
        )
        if cursor.fetchone():
            raise AirflowSkipException("Data already exists in the database")

    return file_path


@op
def read_data_airvisual(context, file_path):
    if not os.path.exists(file_path):
        raise Failure(f"File not found: {file_path}")

    with open(file_path, "r") as f:
        data = json.load(f)

    if "data" not in data:
        raise Failure("Invalid data structure")

    context.log.info(f"Data extraction successful: {data['data']}")
    return data


@op(required_resource_keys={"postgres_resource"})
def load_airvisual_to_postgresql(context, data):
    with context.resources.postgres_resource.cursor() as cursor:
        try:
            # Load pollution and weather mapping configurations
            with open("pollution_mapping.json", "r") as f:
                pollution_mapping = json.load(f)
            with open("weather_mapping.json", "r") as f:
                weather_mapping = json.load(f)

            # Insert timestamp data into dimDateTimeTable
            cursor.execute(
                """
                INSERT INTO dimDateTimeTable (timestamp, year, month, day, hour, minute, second)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
                """,
                (
                    data["data"]["time"],
                    data["data"]["time"].year,
                    data["data"]["time"].month,
                    data["data"]["time"].day,
                    data["data"]["time"].hour,
                    data["data"]["time"].minute,
                    data["data"]["time"].second,
                )
            )

            # Insert location data into dimLocationTable
            cursor.execute(
                """
                INSERT INTO dimLocationTable (latitude, longitude, city, state, country)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (latitude, longitude) DO NOTHING
                """,
                (
                    data["data"]["location"]["coordinates"][0],
                    data["data"]["location"]["coordinates"][1],
                    data["data"]["location"]["city"],
                    data["data"]["location"]["state"],
                    data["data"]["location"]["country"],
                )
            )

            # Insert pollution metadata into dimMainPollutionTable
            cursor.execute(
                """
                INSERT INTO dimMainPollutionTable (pollutant, concentration, aqi)
                VALUES (%s, %s, %s)
                ON CONFLICT (pollutant) DO NOTHING
                """,
                (
                    data["data"]["pollution"]["pm25"],
                    data["data"]["pollution"]["pm25"],
                    data["data"]["pollution"]["aqi"],
                )
            )

            # Insert main fact data into factairvisualtable
            cursor.execute(
                """
                INSERT INTO factairvisualtable (timestamp, latitude, longitude, pollutant, concentration, aqi, weather)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, latitude, longitude, pollutant) DO NOTHING
                """,
                (
                    data["data"]["time"],
                    data["data"]["location"]["coordinates"][0],
                    data["data"]["location"]["coordinates"][1],
                    data["data"]["pollution"]["pm25"],
                    data["data"]["pollution"]["pm25"],
                    data["data"]["pollution"]["aqi"],
                    json.dumps(data["data"]["weather"]),
                )
            )

            context.resources.postgres_resource.commit()
        except Exception as e:
            context.resources.postgres_resource.rollback()
            raise Failure(f"Database operation failed: {e}")


@job(
    resource_defs={
        "airvisual_api": airvisual_api_resource,
        "postgres_resource": postgres_resource,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def airvisual_etl_job():
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)


if __name__ == "__main__":
    result = airvisual_etl_job.execute_in_process()