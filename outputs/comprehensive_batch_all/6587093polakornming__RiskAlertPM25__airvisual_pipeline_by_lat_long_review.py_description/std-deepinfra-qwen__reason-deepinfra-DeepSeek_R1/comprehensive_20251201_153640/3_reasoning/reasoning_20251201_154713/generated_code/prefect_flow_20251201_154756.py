<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements.

First, the pipeline has three tasks that run sequentially: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. Each task is a @task, and the main @flow orchestrates them in order. Since there's no parallelism, I don't need to use .submit(), just call them normally.

For the tasks:

1. get_airvisual_data_hourly: This task fetches data from the AirVisual API. It needs to validate the response, check for duplicates in the database, write JSON temporarily, and skip if data exists. The user mentioned raising AirflowSkipException, but in Prefect, we should use prefect.exceptions.Skip instead. Also, retries are configured here: 2 retries with 5-minute delay. So I'll add retries to the @task decorator.

2. read_data_airvisual: Reads the JSON file, validates it, logs success. This is straightforward. No retries mentioned here, so default.

3. load_airvisual_to_postgresql: Handles ETL to PostgreSQL. Loads configs, inserts into dimension and fact tables with conflict resolution. Uses transaction management. Retries at DAG level: 3-minute delay, but Prefect handles flow-level retries. Maybe set flow-level retries? Wait, the description says "DAG-level retries" which in Prefect would be flow retries. So the flow should have retries=3 (but the description says 2 retries for API task and 3-minute delay for DAG-level). Wait, the retry configuration says: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries. So the API task has retries=2, retry_delay_seconds=300. The flow-level retries would be set with retries and retry_delay_seconds on the flow decorator? Or is that a misunderstanding? The description says "DAG-level retries" which in Airflow would be for the entire DAG, but in Prefect, retries are per-task or per-flow. Since the user mentioned "DAG-level retries", perhaps the flow should have retries=3 and retry_delay_seconds=180. But the original pipeline says "retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So the API task has retries=2, delay=5min, and the flow (DAG-level) has retries=3? Wait, maybe the DAG-level retries refer to the entire flow's retries. So the flow would have retries=3 with a delay of 3 minutes. But Prefect's flow retries are different; each flow run can retry, but tasks have their own retries. Hmm. The user might have intended that the flow has retries=3 with a delay of 3 minutes, and the API task has retries=2 with 5-minute delay. So I need to set the task's retries and the flow's retries accordingly.

But looking at the code constraints: the user says "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So the API task (get_airvisual_data_hourly) should have retries=2 and retry_delay_seconds=300. The DAG-level (flow) retries would be handled by the flow's settings. However, Prefect flows themselves don't have retries; retries are per-task. Unless the user is referring to the flow run retries, which is a different concept. Maybe the flow is configured to retry the entire flow run 3 times with a 3-minute delay. But Prefect doesn't support flow-level retries in the same way as tasks. So perhaps the user intended that the flow has a retry configuration, but in Prefect, that's not directly possible. Alternatively, maybe the tasks have their own retries. The API task has retries=2, retry_delay=5min. The other tasks might not have retries specified. The DAG-level retries might be a misunderstanding, so perhaps ignore that part unless there's a way to handle it. Alternatively, maybe the flow is set with a retry decorator, but Prefect doesn't support that. So perhaps the user made a mistake, and we should focus on the task-level retries.

So, for the get_airvisual_data_hourly task, add retries=2 and retry_delay_seconds=300. The other tasks don't mention retries, so leave them as default. The flow's retries might not be applicable here.

Next, the flow should have catchup disabled. In Prefect, that's handled via the deployment configuration, so maybe add a comment about that. The schedule is manual, so schedule_interval=None, which is the default. The code should include an if __name__ == '__main__': block to run the flow locally.

Now, the tasks:

For get_airvisual_data_hourly, the task needs to check if data exists in the database. If it does, raise Skip to skip downstream tasks. So in Prefect, if this task raises Skip, the subsequent tasks (read and load) won't run. That's correct.

The task also writes JSON temporarily before committing. So perhaps write to a temporary file and then move it atomically. But the code example can use placeholders with comments.

The task uses an API key from config.conf. So in the code, maybe read from a config file, but for the code to be executable, perhaps use os.environ.get or a placeholder with a comment.

Similarly, the PostgreSQL connection is using "postgres_conn" which might be a Prefect block. So in the code, use a placeholder for getting the connection, perhaps using a Prefect SQLAlchemy connector or psycopg2.

The load task reads pollution and weather mappings from JSON files. So code to read those files, but again, use placeholders with comments.

Now, putting it all together:

Imports: from prefect import flow, task. Also, need to import any necessary modules like requests, datetime, json, etc. But the code should assume required packages are installed.

The flow function is named something like air_quality_etl_flow. The tasks are decorated with @task.

Each task has parameters if needed. For example, the get task might need coordinates and api_key. But since the problem states to use fixed coordinates for Salaya, those can be hardcoded or passed as default parameters.

The flow function will call the tasks in order: t1 -> t2 -> t3. Since it's sequential, no .submit() needed.

Now, code structure:

- Imports: prefect, necessary modules.
- Define tasks with @task decorators, including retries where needed.
- Flow function with @flow, maybe with retries if applicable (but not sure).
- In the flow, call the tasks in order.
- If __name__ == '__main__': call the flow.

Handling the database checks: For the get task, check if data exists. If yes, raise Skip. How to interact with the database? Use a placeholder function, maybe with a comment indicating that the actual implementation would query the database.

Similarly, the load task inserts into multiple tables with conflict resolution. Use placeholders for the actual SQL operations, but structure the code with comments.

Now, code:

Start with imports:

from prefect import flow, task
from prefect.exceptions import Skip
from datetime import datetime, timedelta
import requests
import json
import os
from zoneinfo import ZoneInfo
# Other necessary imports like psycopg2 or SQLAlchemy?

Then, define the tasks.

For get_airvisual_data_hourly:

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    # Fetch data from API
    api_key = os.getenv("AIRVISUAL_API_KEY")  # Assume API key is in environment
    lat = 13.7898  # Salaya coordinates
    lon = 100.3211
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    timestamp = datetime.fromtimestamp(data['data']['current']['pollution']['ts'], ZoneInfo("Asia/Bangkok"))
    
    # Check if data exists in database (placeholder)
    # Assume a function check_existing_data(timestamp) returns True if exists
    if check_existing_data(timestamp):
        raise Skip("Data already exists, skipping downstream tasks")
    
    # Atomic write to temporary file
    temp_path = "temp_airvisual.json"
    final_path = "airvisual_data.json"
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.rename(temp_path, final_path)
    
    return final_path

But the check_existing_data function is not defined. So perhaps add a placeholder or comment.

But the code needs to be executable. So maybe mock that part with a comment.

Alternatively, use a placeholder function that's not implemented but returns False for the sake of the code.

But the user said to avoid placeholders like TODO. So perhaps include a minimal implementation.

Wait, but the code must be executable. So perhaps the check_existing_data is a helper function that connects to the DB and checks. But connecting to PostgreSQL requires credentials. Since the user mentioned using the connection ID "postgres_conn", perhaps use Prefect's PostgresConnector block.

But to keep the code self-contained, maybe use a placeholder with a comment.

Alternatively, use a dummy check that always returns False for the sake of example.

But the user said to avoid placeholders. So perhaps:

def check_existing_data(timestamp):
    # Placeholder: Implement actual database check here
    return False

But including this function inside the task or as a helper.

Wait, but tasks can't have nested functions. So perhaps define the helper function outside the task.

But the code must be executable. So perhaps:

In the task, after getting the timestamp, check against the database. For example:

from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    # ... fetch data ...
    timestamp = ...
    # Check database for existing data
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        with connector.get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM factairvisualtable WHERE timestamp = %s)",
                (timestamp,)
            )
            exists = cursor.fetchone()[0]
    if exists:
        raise Skip(...)
    # ...

But this requires prefect-sqlalchemy and a configured block. Since the user wants the code to be executable (assuming required packages are installed), including this makes sense. However, the user might not have prefect-sqlalchemy installed. Alternatively, use psycopg2.

But the code should be minimal. So perhaps use a placeholder with a comment and a dummy check.

Alternatively, proceed with the SqlAlchemyConnector approach.

But the user's code may not have that block configured. So perhaps the code will raise an error unless the block is set up, but the code is structurally correct.

Alternatively, use a try-except block to mock the check.

But given the constraints, perhaps proceed with the actual code, assuming that the user has the PostgresConnector block configured with the id "postgres_conn".

So, the code would include:

from prefect_sqlalchemy import SqlAlchemyConnector

Then, in the task:

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    # ... API call ...
    # Check if data exists in database
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        connection = connector.get_connection()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 FROM factairvisualtable WHERE timestamp = %s",
            (timestamp,)
        )
        if cursor.fetchone() is not None:
            connection.close()
            raise Skip("Data exists, skipping")
        connection.close()

But this requires the factairvisualtable to have a timestamp column. The code is just an example.

Similarly, the load task would use the same connector.

Now, the load_airvisual_to_postgresql task:

@task
def load_airvisual_to_postgresql(data_path):
    # Load data from JSON
    with open(data_path) as f:
        data = json.load(f)
    
    # Load pollution and weather mappings
    with open("pollution_mappings.json") as f:
        pollution_mappings = json.load(f)
    with open("weather_mappings.json") as f:
        weather_mappings = json.load(f)
    
    # Insert into dimDateTimeTable
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        connection = connector.get_connection()
        cursor = connection.cursor()
        try:
            # Insert into dimDateTimeTable
            timestamp = ...  # Extract from data
            cursor.execute(
                """
                INSERT INTO dimDateTimeTable (timestamp)
                VALUES (%s)
                ON CONFLICT (timestamp) DO NOTHING
                """,
                (timestamp,)
            )
            # Similar for other tables
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            connection.close()

But this is a simplified version.

Putting it all together, the code would look like:

from prefect import flow, task
from prefect.exceptions import Skip
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import datetime
import requests
import json
import os
from zoneinfo import ZoneInfo

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    api_key = os.getenv("AIRVISUAL_API_KEY")
    if not api_key:
        raise ValueError("AIRVISUAL_API_KEY environment variable not set")
    
    lat, lon = 13.7898, 100.3211  # Salaya coordinates
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    ts = data['data']['current']['pollution']['ts']
    timestamp = datetime.fromtimestamp(ts, ZoneInfo("Asia/Bangkok"))
    
    # Check for existing data
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        with connector.get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT 1 FROM factairvisualtable WHERE timestamp = %s",
                (timestamp,)
            )
            if cursor.fetchone() is not None:
                raise Skip("Data exists, skipping downstream tasks")
    
    # Atomic write
    temp_path = "temp_airvisual.json"
    final_path = "airvisual_data.json"
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.replace(temp_path, final_path)  # Atomic on Unix
    
    return final_path

@task
def read_data_airvisual(data_path):
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file {data_path} not found")
    
    with open(data_path) as f:
        data = json.load(f)
    
    # Validate data structure
    required_keys = ['data', 'status']
    if not all(k in data for k in required_keys) or data['status'] != 'success':
        raise ValueError("Invalid data structure")
    
    print("Data extraction verified successfully")
    return data

@task
def load_airvisual_to_postgresql(data):
    # Load mappings
    with open("pollution_mappings.json") as f:
        pollution_mappings = json.load(f)
    with open("weather_mappings.json") as f:
        weather_mappings = json.load(f)
    
    # Extract necessary data
    current = data['data']['current']
    pollution = current['pollution']
    weather = current['weather']
    timestamp = datetime.fromtimestamp(pollution['ts'], ZoneInfo("Asia/Bangkok"))
    
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        with connector.get_connection() as connection:
            cursor = connection.cursor()
            try:
                # Insert into dimDateTimeTable
                cursor.execute(
                    """
                    INSERT INTO dimDateTimeTable (timestamp)
                    VALUES (%s)
                    ON CONFLICT (timestamp) DO NOTHING
                    """,
                    (timestamp,)
                )
                
                # Insert into dimLocationTable
                location_data = data['data']['city']
                cursor.execute(
                    """
                    INSERT INTO dimLocationTable (city, country)
                    VALUES (%s, %s)
                    ON CONFLICT (city, country) DO NOTHING
                    """,
                    (location_data['city'], location_data['country'])
                )
                
                # Insert into dimMainPollutionTable
                main_pollutant = pollution['mainus']
                cursor.execute(
                    """
                    INSERT INTO dimMainPollutionTable (pollutant_name)
                    VALUES (%s)
                    ON CONFLICT (pollutant_name) DO NOTHING
                    """,
                    (main_pollutant,)
                )
                
                # Insert into factairvisualtable
                # Assuming columns exist; adjust as per schema
                cursor.execute(
                    """
                    INSERT INTO factairvisualtable (
                        timestamp_id, location_id, pollutant_id,
                        aqi, temperature, humidity, pressure
                    )
                    SELECT dt.id, loc.id, mp.id, %s, %s, %s, %s
                    FROM dimDateTimeTable dt
                    JOIN dimLocationTable loc ON loc.city = %s AND loc.country = %s
                    JOIN dimMainPollutionTable mp ON mp.pollutant_name = %s
                    WHERE dt.timestamp = %s
                    """,
                    (
                        pollution['aqius'], weather['tp'], weather['hu'], weather['pr'],
                        location_data['city'], location_data['country'],
                        main_pollutant, timestamp
                    )
                )
                
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise e

@flow(retries=3, retry_delay_seconds=180)
def air_quality_etl_flow():
    data_path = get_airvisual_data_hourly()
    data = read_data_airvisual(data_path)
    load_airvisual_to_postgresql(data)

if __name__ == "__main__":
    air_quality_etl_flow()

Wait, but the flow's retries are set to 3 with 3-minute delay. The original problem says "3-minute delay for DAG-level retries". So adding retries=3 and retry_delay_seconds=180 to the flow's decorator.

But in Prefect, flow retries are not the same as task retries. Flow retries would retry the entire flow run if it fails. Each task has its own retries. So the flow's retries are separate from the tasks'. The original problem mentions "DAG-level retries" which might correspond to flow retries. So adding @flow(retries=3, retry_delay_seconds=180) makes sense.

But the user's description says "retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So the API task has retries=2, retry_delay=5*60=300, and the flow has retries=3, retry_delay=3*60=180. So the code includes that.

Now, check if all steps are covered:

- Tasks are defined with @task, flow with @flow.
- Sequential execution.
- get_airvisual_data_hourly raises Skip if data exists, which skips downstream tasks.
- Uses SqlAlchemyConnector for PostgreSQL.
- Handles transactions with commit/rollback.
- Atomic file write with os.replace.
- Configuration via environment variables (API key).
- Fixed coordinates.

Potential issues:

- The check for existing data in get_airvisual_data_hourly uses factairvisualtable's timestamp. But maybe the dimDateTimeTable is where the timestamp is stored. But the problem description says the first task checks for existing data in the database to avoid duplicates. The exact table isn't specified, so using fact table is a guess.

- The load task's SQL statements are placeholders. They assume certain table structures, which may not match the actual schema. But the problem description mentions the tables, so the code includes inserts into those tables with conflict resolution.

- The code uses prefect_sqlalchemy, which requires the package prefect-sqlalchemy. The user may need to install it, but the code is correct assuming that.

- The timezone conversion uses zoneinfo, which is available in Python 3.9+. So the code should have from zoneinfo import ZoneInfo, and the environment must have that available.

- The data extraction in get_airvisual_data_hourly assumes the API response structure. If the actual response differs, it may fail. But the code is based on the problem description.

Overall, the code should be executable with the right dependencies and configurations (API key, Postgres block, JSON files).
</think>

from prefect import flow, task
from prefect.exceptions import Skip
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import datetime
import requests
import json
import os
from zoneinfo import ZoneInfo

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    """Fetch air quality data from AirVisual API with deduplication check."""
    api_key = os.getenv("AIRVISUAL_API_KEY")
    lat, lon = 13.7898, 100.3211  # Salaya coordinates
    
    try:
        response = requests.get(
            f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
        )
        response.raise_for_status()
        data = response.json()
        
        ts = data['data']['current']['pollution']['ts']
        timestamp = datetime.fromtimestamp(ts, ZoneInfo("Asia/Bangkok"))
        
        with SqlAlchemyConnector.load("postgres_conn") as connector:
            with connector.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM factairvisualtable WHERE timestamp = %s",
                    (timestamp,)
                )
                if cursor.fetchone():
                    raise Skip("Duplicate data found - skipping downstream tasks")
        
        temp_path = "temp_airvisual.json"
        final_path = "airvisual_data.json"
        with open(temp_path, 'w') as f:
            json.dump(data, f)
        os.replace(temp_path, final_path)
        
        return final_path
        
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e

@task
def read_data_airvisual(data_path):
    """Validate extracted data file structure."""
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file {data_path} missing")
    
    with open(data_path) as f:
        data = json.load(f)
    
    if not all(key in data for key in ['data', 'status']) or data['status'] != 'success':
        raise ValueError("Invalid API response structure")
    
    return data

@task
def load_airvisual_to_postgresql(data):
    """Load transformed data into PostgreSQL with transaction management."""
    with SqlAlchemyConnector.load("postgres_conn") as connector:
        with connector.get_connection() as conn:
            cursor = conn.cursor()
            try:
                current = data['data']['current']
                pollution = current['pollution']
                weather = current['weather']
                ts = pollution['ts']
                timestamp = datetime.fromtimestamp(ts, ZoneInfo("Asia/Bangkok"))
                
                # Insert dimension data
                cursor.execute("""
                    INSERT INTO dimDateTimeTable (timestamp)
                    VALUES (%s) ON CONFLICT (timestamp) DO NOTHING
                """, (timestamp,))
                
                location = data['data']['city']
                cursor.execute("""
                    INSERT INTO dimLocationTable (city, country)
                    VALUES (%s, %s) ON CONFLICT (city, country) DO NOTHING
                """, (location['city'], location['country']))
                
                cursor.execute("""
                    INSERT INTO dimMainPollutionTable (pollutant_name)
                    VALUES (%s) ON CONFLICT (pollutant_name) DO NOTHING
                """, (pollution['mainus'],))
                
                # Insert fact data
                cursor.execute("""
                    INSERT INTO factairvisualtable (
                        timestamp_id, location_id, pollutant_id,
                        aqi, temperature, humidity, pressure
                    )
                    SELECT dt.id, loc.id, pol.id, %s, %s, %s, %s
                    FROM dimDateTimeTable dt
                    CROSS JOIN dimLocationTable loc
                    CROSS JOIN dimMainPollutionTable pol
                    WHERE dt.timestamp = %s
                    AND loc.city = %s AND loc.country = %s
                    AND pol.pollutant_name = %s
                """, (
                    pollution['aqius'], weather['tp'], weather['hu'], weather['pr'],
                    timestamp, location['city'], location['country'], pollution['mainus']
                ))
                
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

@flow(retries=3, retry_delay_seconds=180)
def air_quality_etl_flow():
    """Main ETL flow for air quality data processing."""
    data_path = get_airvisual_data_hourly()
    validated_data = read_data_airvisual(data_path)
    load_airvisual_to_postgresql(validated_data)

if __name__ == "__main__":
    air_quality_etl_flow()