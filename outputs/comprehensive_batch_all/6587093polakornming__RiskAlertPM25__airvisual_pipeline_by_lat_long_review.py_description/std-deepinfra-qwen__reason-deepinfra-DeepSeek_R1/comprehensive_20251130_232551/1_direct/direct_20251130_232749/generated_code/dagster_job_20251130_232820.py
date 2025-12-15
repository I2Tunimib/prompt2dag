from dagster import op, job, RetryPolicy, Failure, Field, String, resource
from datetime import datetime, timezone
import pytz
import requests
import json
import os
import psycopg2
from airflow.exceptions import AirflowSkipException

# Resources
@resource(config_schema={"conn_id": Field(String, default_value="postgres_conn")})
def postgres_resource(context):
    conn_info = context.resource_config
    conn = psycopg2.connect(conn_info["conn_id"])
    try:
        yield conn
    finally:
        conn.close()

# Ops
@op(required_resource_keys={"postgres"})
def get_airvisual_data_hourly(context):
    api_key = "your_api_key"
    latitude = 13.7563
    longitude = 100.3182
    url = f"https://api.airvisual.com/v2/nearest_city?lat={latitude}&lon={longitude}&key={api_key}"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    if data["status"] != "success":
        raise Failure(f"API request failed: {data['data']}")
    
    data["data"]["time"] = pytz.timezone("Asia/Bangkok").localize(
        datetime.fromtimestamp(data["data"]["time"]["s"])
    ).isoformat()
    
    with open("airvisual_data.json", "w") as f:
        json.dump(data, f)
    
    with context.resources.postgres.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM factairvisualtable WHERE timestamp = %s",
            (data["data"]["time"],)
        )
        count = cursor.fetchone()[0]
    
    if count > 0:
        raise AirflowSkipException("Data already exists for this hour")
    
    return data

@op
def read_data_airvisual(context):
    if not os.path.exists("airvisual_data.json"):
        raise Failure("JSON file not found")
    
    with open("airvisual_data.json", "r") as f:
        data = json.load(f)
    
    if "data" not in data:
        raise Failure("Invalid data structure")
    
    context.log.info("Data extraction successful")
    return data

@op(required_resource_keys={"postgres"})
def load_airvisual_to_postgresql(context, data):
    with context.resources.postgres.cursor() as cursor:
        # Load pollution and weather mappings
        with open("pollution_mapping.json", "r") as f:
            pollution_mapping = json.load(f)
        with open("weather_mapping.json", "r") as f:
            weather_mapping = json.load(f)
        
        # Insert timestamp data
        cursor.execute(
            """
            INSERT INTO dimDateTimeTable (timestamp, date, hour)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING
            """,
            (data["data"]["time"], data["data"]["time"].split("T")[0], data["data"]["time"].split("T")[1][:2])
        )
        
        # Insert location data
        cursor.execute(
            """
            INSERT INTO dimLocationTable (latitude, longitude, city, country)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (latitude, longitude) DO NOTHING
            """,
            (data["data"]["location"]["coordinates"][1], data["data"]["location"]["coordinates"][0], data["data"]["city"], data["data"]["country"])
        )
        
        # Insert pollution metadata
        cursor.execute(
            """
            INSERT INTO dimMainPollutionTable (pollution_index, main_pollutant, aqi)
            VALUES (%s, %s, %s)
            ON CONFLICT (pollution_index) DO NOTHING
            """,
            (data["data"]["pollution"]["aqi"], data["data"]["pollution"]["main"], data["data"]["pollution"]["aqi"])
        )
        
        # Insert main fact data
        cursor.execute(
            """
            INSERT INTO factairvisualtable (timestamp_id, location_id, pollution_id, temperature, humidity, pressure, wind_speed, wind_direction)
            VALUES (
                (SELECT id FROM dimDateTimeTable WHERE timestamp = %s),
                (SELECT id FROM dimLocationTable WHERE latitude = %s AND longitude = %s),
                (SELECT id FROM dimMainPollutionTable WHERE pollution_index = %s),
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (timestamp_id, location_id) DO NOTHING
            """,
            (
                data["data"]["time"],
                data["data"]["location"]["coordinates"][1],
                data["data"]["location"]["coordinates"][0],
                data["data"]["pollution"]["aqi"],
                data["data"]["weather"]["tp"],
                data["data"]["weather"]["hu"],
                data["data"]["weather"]["pr"],
                data["data"]["weather"]["ws"],
                data["data"]["weather"]["wd"]
            )
        )
        
        context.resources.postgres.commit()

# Job
@job(
    resource_defs={"postgres": postgres_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def airvisual_etl_job():
    data = get_airvisual_data_hourly()
    read_data_airvisual(data)
    load_airvisual_to_postgresql(data)

if __name__ == "__main__":
    result = airvisual_etl_job.execute_in_process()