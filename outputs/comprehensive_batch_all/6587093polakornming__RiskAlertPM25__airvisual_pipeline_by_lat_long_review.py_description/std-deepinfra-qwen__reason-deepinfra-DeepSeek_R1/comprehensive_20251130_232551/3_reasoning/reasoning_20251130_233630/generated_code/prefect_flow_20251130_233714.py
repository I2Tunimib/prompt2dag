<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let me start by understanding the requirements and constraints.

First, the pipeline has three tasks that run sequentially: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. Each task should be a Prefect task decorated with @task, and the main flow will be a @flow that orchestrates them in order.

The user mentioned using .submit() for parallel steps, but since the orchestration here is sequential, I don't need to use submit. Just call the tasks normally.

For the get_airvisual_data_hourly task, there are several steps: fetching data from the API, validating the response, checking for duplicates in the database, writing to a temporary JSON file, and raising an exception if data exists. The exception mentioned is AirflowSkipException, but since this is Prefect, I should use prefect.exceptions.SkipFlow instead to skip downstream tasks.

Next, the read_data_airvisual task reads the JSON file, validates it, and logs success. Then load_airvisual_to_postgresql handles the ETL to PostgreSQL, including loading configs, inserting into dimension tables, and managing transactions.

I need to handle retries. The API task has 2 retries with a 5-minute delay, and the flow (DAG-level) has retries with 3-minute delay. In Prefect, task retries are set with retries and retry_delay_seconds parameters in the @task decorator. For the flow, maybe use a flow decorator parameter like retries, but the description says "DAG-level retries", which might mean flow-level retries. However, Prefect flows don't have retries in the same way; perhaps the user meant task retries. Wait, the task get_airvisual has 2 retries, and the DAG (flow) has retries with 3-minute delay. Maybe that's a mistake, but I'll implement the task retries as specified.

Now, for the code structure:

- Import necessary modules: prefect, datetime, json, os, etc.
- Define the tasks with @task, including their parameters like retries.
- The flow function will call each task in order.
- Use Prefect's SQLAlchemy blocks for PostgreSQL connection. The user mentioned connection ID "postgres_conn", which in Prefect 2.x is a block. So I'll load the PostgreSQL connection from a block.
- Configuration files: API key from config.conf. Since Prefect recommends using blocks or environment variables for configurations, maybe the API key is stored in a block or as an environment variable. But the user says to assume required packages are installed, so perhaps the code reads from a local config file. However, for security, it's better to use Prefect's Secret blocks. But the user's description says "Configuration Files: API keys from config.conf", so I'll proceed with reading from a config file. But in the code, maybe use a placeholder with a comment.
- The tasks need to interact with the file system for temporary JSON and config mappings. The get task writes a temp file, then the read task reads it. So the file path should be passed between tasks or stored in a known location.
- For database operations, use SQLAlchemy with the postgres_conn block. Each insert should handle conflicts, perhaps using ON CONFLICT DO NOTHING or similar.
- Transaction management: Use a database transaction that commits only if all steps succeed, otherwise rollback.

Potential issues:

- Handling the database connection: Since tasks are separate, the connection might need to be passed or reinitialized in each task. But using a block, each task can load the connection.
- The atomic file write: The get task writes to a temp file and then renames it atomically. Use a temporary file and then os.rename to overwrite the target file.
- Checking for existing data: The get task checks if the current hour's data exists in the DB. That requires querying the database, which might need a connection. So the get task would need to connect to the DB as well.
- Timezone conversion: Convert the timestamp to Bangkok time. Use pytz or zoneinfo (Python 3.9+). Assume that the API returns a UTC timestamp, convert it to Asia/Bangkok.

Putting it all together:

Start with imports, then define the tasks, then the flow.

For the get_airvisual_data_hourly task:

- Fetch data from API using requests.get (need to import requests).
- API key is read from config.conf. So in code, maybe use a configparser to read from 'config.conf', but since the user didn't provide the actual config structure, perhaps mock it with a placeholder and add a comment.
- Validate response status code and JSON structure.
- Convert timestamp to Bangkok timezone.
- Check database for existing record in dimDateTimeTable for the current hour. If exists, raise SkipFlow.
- Write data to a temp file, then rename atomically.

For read_data_airvisual:

- Read the JSON file written by the previous task.
- Validate the file exists and data structure is correct.

For load_airvisual_to_postgresql:

- Load pollution and weather mappings from JSON files.
- Use SQLAlchemy to insert into various tables, handling conflicts.
- Use a transaction to ensure all inserts are atomic.

Retry configuration: Add retries=2 and retry_delay_seconds=300 to the get task's @task decorator. The flow's retries are mentioned as DAG-level with 3-minute delay. Since Prefect flows can have retries, but typically tasks handle their own retries. Maybe the user intended task-level retries. So perhaps the flow doesn't have retries, but the tasks do. But the description says "DAG-level retries: 3-minute delay". Maybe that's a confusion with Airflow terms. In Prefect, flows can have retries, but it's less common. Alternatively, maybe the flow is set with a retry configuration. However, the user's constraint says to include retry configuration as per the description. So perhaps the flow has a retry configuration. Wait, the original pipeline's retry config: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries. So the API task (get_airvisual) has retries=2, delay 5 min. The flow (DAG-level) has retries with 3 min delay. But Prefect flows can have retries? Or maybe it's a mistake and the other tasks have retries. Hmm. The user's instruction says "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So perhaps the flow itself is configured with retries, but Prefect flows don't retry the entire flow by default. Maybe this is a misunderstanding, and the intended retries are for the tasks. Alternatively, maybe the other tasks have retries. But the description only mentions retries for the API task and DAG-level. Since the user is converting from Airflow, perhaps DAG-level retries refer to flow-level retries. But Prefect doesn't support flow-level retries in the same way. So perhaps the user intended that the flow has a retry configuration, but in Prefect, that's not standard. Maybe the user made a mistake, and the DAG-level retries are for the flow, but since Prefect doesn't support that, perhaps we can ignore it or add a comment. Alternatively, maybe the other tasks have retries. But the description says "retries: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". Since the API task is the only one with specific retries, perhaps the other tasks don't have retries. So in code, set retries=2 and retry_delay_seconds=300 for the get task. The DAG-level (flow) retries might be a confusion, so perhaps ignore that or add a comment.

Now, code structure:

Import necessary modules:

from prefect import flow, task
from prefect.blocks.sqlalchemy import SqlAlchemyConnector
from prefect.exceptions import SkipFlow
from datetime import datetime
import pytz
import requests
import json
import os
from sqlalchemy import text, create_engine
from typing import Dict, Any

Then define the tasks.

For the get task:

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    # Fetch API key from config
    # Assuming config.conf is in the same directory
    # For production, use Prefect Secrets or environment variables
    api_key = "YOUR_API_KEY"  # Replace with actual retrieval method
    lat = 13.7891
    lon = 100.3223
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    timestamp_utc = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
    tz = pytz.timezone('Asia/Bangkok')
    timestamp_bkk = timestamp_utc.astimezone(tz)
    
    # Check database for existing record
    postgres_block = SqlAlchemyConnector.load("postgres_conn")
    with postgres_block.get_connection() as engine:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT 1 FROM dimDateTimeTable WHERE timestamp = :ts"),
                {'ts': timestamp_bkk}
            ).fetchone()
            if result:
                raise SkipFlow("Data already exists for current hour")
    
    # Atomic file write
    temp_path = 'temp_airvisual_data.json'
    final_path = 'airvisual_data.json'
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.rename(temp_path, final_path)
    
    return final_path

Wait, but the task returns the file path, which is then used by the next task. So the read_data_airvisual task would take that path as input.

But in Prefect, if tasks are called sequentially, the output of one task is passed as input to the next. So in the flow, it would be:

@flow(retries=3, retry_delay_seconds=180)  # Assuming DAG-level retries refer to flow retries
def air_quality_etl():
    data_path = get_airvisual_data_hourly()
    verified_data = read_data_airvisual(data_path)
    load_result = load_airvisual_to_postgresql(verified_data)

But wait, the user's description says "DAG-level retries: 3-minute delay". So perhaps the flow has retries=3 and retry_delay_seconds=180. But Prefect allows flow retries? Let me check. Yes, in Prefect 2, flows can have retries. So adding @flow(retries=3, retry_delay_seconds=180) would configure flow-level retries.

But the original description says "retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So the API task (get_airvisual) has retries=2, delay 300s, and the flow has retries=3, delay 180s. So in code, the flow decorator would have retries=3 and retry_delay_seconds=180. But I need to confirm if Prefect allows flow retries. According to Prefect docs, flows can have retry settings. So adding that to the flow decorator.

Now, the read_data_airvisual task:

@task
def read_data_airvisual(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file {file_path} not found")
    with open(file_path, 'r') as f:
        data = json.load(f)
    # Validate data structure
    required_keys = ['data', 'status']
    if not all(k in data for k in required_keys):
        raise ValueError("Invalid data structure")
    # Log success
    print("Data extraction verified successfully")  # Or use Prefect logger
    return data

Then the load task:

@task
def load_airvisual_to_postgresql(data: Dict[str, Any]):
    # Load mappings
    with open('pollution_mappings.json') as f:
        pollution_mappings = json.load(f)
    with open('weather_mappings.json') as f:
        weather_mappings = json.load(f)
    
    postgres_block = SqlAlchemyConnector.load("postgres_conn")
    with postgres_block.get_connection() as engine:
        with engine.begin() as conn:
            # Insert into dimDateTimeTable
            timestamp_bkk = datetime.fromisoformat(data['data']['current']['pollution']['ts']).astimezone(pytz.timezone('Asia/Bangkok'))
            conn.execute(
                text("""
                    INSERT INTO dimDateTimeTable (timestamp, hour, day, month, year)
                    VALUES (:ts, EXTRACT(HOUR FROM :ts), EXTRACT(DAY FROM :ts), EXTRACT(MONTH FROM :ts), EXTRACT(YEAR FROM :ts))
                    ON CONFLICT (timestamp) DO NOTHING
                """),
                {'ts': timestamp_bkk}
            )
            
            # Insert into dimLocationTable
            loc_data = data['data']['location']['coordinates']
            conn.execute(
                text("""
                    INSERT INTO dimLocationTable (latitude, longitude, city, country)
                    VALUES (:lat, :lon, :city, :country)
                    ON CONFLICT (latitude, longitude) DO NOTHING
                """),
                {
                    'lat': loc_data[1],
                    'lon': loc_data[0],
                    'city': data['data']['city'],
                    'country': data['data']['country']
                }
            )
            
            # Insert into dimMainPollutionTable
            pollution_data = data['data']['current']['pollution']
            conn.execute(
                text("""
                    INSERT INTO dimMainPollutionTable (aqius, aqicn, mainus, maincn)
                    VALUES (:aqius, :aqicn, :mainus, :maincn)
                    ON CONFLICT (aqius, aqicn) DO NOTHING
                """),
                {
                    'aqius': pollution_data['aqius'],
                    'aqicn': pollution_data['aqicn'],
                    'mainus': pollution_data['mainus'],
                    'maincn': pollution_data['maincn']
                }
            )
            
            # Insert into factairvisualtable
            # Assuming foreign keys are handled, get IDs or use existing
            # For simplicity, this example skips fetching generated IDs
            # You'd typically need to query the dim tables for the inserted IDs
            # This is a simplified version
            weather_data = data['data']['current']['weather']
            conn.execute(
                text("""
                    INSERT INTO factairvisualtable (
                        datetime_id, location_id, pollution_id,
                        temperature, humidity, wind_speed, pressure
                    )
                    VALUES (
                        (SELECT id FROM dimDateTimeTable WHERE timestamp = :ts),
                        (SELECT id FROM dimLocationTable WHERE latitude = :lat AND longitude = :lon),
                        (SELECT id FROM dimMainPollutionTable WHERE aqius = :aqius AND aqicn = :aqicn),
                        :temp, :humidity, :wind_speed, :pressure
                    )
                """),
                {
                    'ts': timestamp_bkk,
                    'lat': loc_data[1],
                    'lon': loc_data[0],
                    'aqius': pollution_data['aqius'],
                    'aqicn': pollution_data['aqicn'],
                    'temp': weather_data['tp'],
                    'humidity': weather_data['hu'],
                    'wind_speed': weather_data['ws'],
                    'pressure': weather_data['pr']
                }
            )
    
    print("Data loaded successfully into PostgreSQL")

Potential issues here: The code assumes that the dim tables have entries and uses subqueries to get the IDs. In reality, if there's a conflict and the insert is skipped, the subquery would find the existing ID. This should work with ON CONFLICT DO NOTHING.

Also, the data['data']['current']['pollution']['ts'] is a timestamp, but in the get task, it was converted to a datetime object. However, when saving to JSON, datetime objects are not serializable, so perhaps the original data's 'ts' is a Unix timestamp. So in the get task, when converting, perhaps the original data's 'ts' is stored as a number, and when writing to JSON, it's saved as a number. Then, when reading back, it's a number again. So in the load task, need to parse that timestamp correctly.

Wait, in the get task, after converting to Bangkok time, the code saves the original data (from the API response) to the JSON file. The API's 'ts' is a Unix timestamp. So when the data is saved to JSON, the 'ts' remains a number. Then, in the load task, when accessing data['data']['current']['pollution']['ts'], it's a number. So converting that to a datetime requires using datetime.fromtimestamp(ts_value).

So in the load task:

timestamp_utc = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
timestamp_bkk = timestamp_utc.astimezone(pytz.timezone('Asia/Bangkok'))

But in the code above, I used datetime.fromisoformat, which would fail because 'ts' is a Unix timestamp (integer). So that's a mistake. Need to fix that.

So in the load task:

pollution_data = data['data']['current']['pollution']
ts = pollution_data['ts']
timestamp_utc = datetime.utcfromtimestamp(ts)
timestamp_bkk = timestamp_utc.astimezone(pytz.timezone('Asia/Bangkok'))

Then use timestamp_bkk in the SQL parameters.

So correcting that part in the load task.

Another issue: The get task writes the entire API response to the JSON file, including the 'ts' field as a number. Then, when read_data_airvisual reads it, the 'ts' is present as a number. So in the load task, when accessing pollution_data['ts'], it's a number.

So the code in the load task's dimDateTimeTable insert should use the correct timestamp.

So in the load task:

timestamp_utc = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
timestamp_bkk = timestamp_utc.astimezone(pytz.timezone('Asia/Bangkok'))

Then pass 'ts' as timestamp_bkk.

Now, putting all together, here's the code:

But also, the user mentioned that the get task "implements atomic file write pattern to save JSON data temporarily before committing". So in the get task, write to a temp file and then rename it. The code does that with temp_path and final_path.

Other considerations:

- The get task raises SkipFlow if data exists. In Prefect, raising SkipFlow will skip all subsequent tasks. So if the data exists, the flow stops after the get task.

- The load task uses SQLAlchemy's text() for raw SQL. Transactions are managed via the context manager (with engine.begin()).

- The PostgreSQL connection is loaded from the "postgres_conn" block, which should be configured in Prefect's UI or via code.

- The main function is guarded by if __name__ == '__main__': air_quality_etl()

Potential missing parts:

- Error handling in database operations. For example, if inserting into dimDateTimeTable fails, the transaction should roll back. The use of 'with engine.begin()' as a context manager ensures that transactions are committed on success or rolled back on exception.

- The mappings for pollution and weather are loaded from JSON files. The code assumes these files exist. If not, the task will raise an error.

- The API key is read from config.conf. In practice, this should be handled via Prefect secrets or environment variables, but the code uses a placeholder.

Now, putting all together into code.
</think>

from prefect import flow, task
from prefect.blocks.sqlalchemy import SqlAlchemyConnector
from prefect.exceptions import SkipFlow
from datetime import datetime
import pytz
import requests
import json
import os
from sqlalchemy import text
from typing import Dict, Any

@task(retries=2, retry_delay_seconds=300)
def get_airvisual_data_hourly():
    """Fetch air quality data from AirVisual API and perform duplicate check"""
    # Configuration (should use Prefect Blocks or secrets in production)
    api_key = "YOUR_API_KEY"  # Replace with actual API key retrieval
    lat = 13.7891
    lon = 100.3223
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise RuntimeError(f"API request failed: {str(e)}") from e

    # Convert timestamp to Bangkok timezone
    ts = data['data']['current']['pollution']['ts']
    timestamp_utc = datetime.utcfromtimestamp(ts)
    timestamp_bkk = timestamp_utc.astimezone(pytz.timezone('Asia/Bangkok'))

    # Check for existing data in database
    postgres_block = SqlAlchemyConnector.load("postgres_conn")
    with postgres_block.get_connection() as engine:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT 1 FROM dimDateTimeTable WHERE timestamp = :ts"),
                {'ts': timestamp_bkk}
            ).fetchone()
            if result:
                raise SkipFlow("Data exists - skipping processing")

    # Atomic file write
    temp_path = "temp_airvisual.json"
    final_path = "airvisual_data.json"
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.replace(temp_path, final_path)

    return final_path

@task
def read_data_airvisual(file_path: str) -> Dict[str, Any]:
    """Validate and return extracted data file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file missing: {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    if 'data' not in data or 'current' not in data['data']:
        raise ValueError("Invalid data structure")
    
    return data

@task
def load_airvisual_to_postgresql(data: Dict[str, Any]):
    """Load validated data into PostgreSQL with transaction management"""
    # Load mapping configurations
    with open('pollution_mappings.json') as f:
        pollution_mappings = json.load(f)
    with open('weather_mappings.json') as f:
        weather_mappings = json.load(f)

    postgres_block = SqlAlchemyConnector.load("postgres_conn")
    with postgres_block.get_connection() as engine:
        with engine.begin() as conn:
            # Extract timestamp data
            ts = data['data']['current']['pollution']['ts']
            timestamp_bkk = datetime.utcfromtimestamp(ts).astimezone(pytz.timezone('Asia/Bangkok'))

            # Insert dimension data
            conn.execute(
                text("""
                    INSERT INTO dimDateTimeTable (timestamp)
                    VALUES (:ts)
                    ON CONFLICT (timestamp) DO NOTHING
                """),
                {'ts': timestamp_bkk}
            )

            loc = data['data']['location']['coordinates']
            conn.execute(
                text("""
                    INSERT INTO dimLocationTable (latitude, longitude)
                    VALUES (:lat, :lon)
                    ON CONFLICT (latitude, longitude) DO NOTHING
                """),
                {'lat': loc[1], 'lon': loc[0]}
            )

            pollution = data['data']['current']['pollution']
            conn.execute(
                text("""
                    INSERT INTO dimMainPollutionTable (aqius, mainus)
                    VALUES (:aqius, :mainus)
                    ON CONFLICT (aqius) DO NOTHING
                """),
                {'aqius': pollution['aqius'], 'mainus': pollution['mainus']}
            )

            # Insert fact data
            weather = data['data']['current']['weather']
            conn.execute(
                text("""
                    INSERT INTO factairvisualtable (
                        datetime_id, location_id, pollution_id,
                        temperature, humidity, wind_speed
                    )
                    SELECT
                        (SELECT id FROM dimDateTimeTable WHERE timestamp = :ts),
                        (SELECT id FROM dimLocationTable WHERE latitude = :lat AND longitude = :lon),
                        (SELECT id FROM dimMainPollutionTable WHERE aqius = :aqius),
                        :temp, :humidity, :wind_speed
                """),
                {
                    'ts': timestamp_bkk,
                    'lat': loc[1],
                    'lon': loc[0],
                    'aqius': pollution['aqius'],
                    'temp': weather['tp'],
                    'humidity': weather['hu'],
                    'wind_speed': weather['ws']
                }
            )

@flow(retries=3, retry_delay_seconds=180)
def air_quality_etl():
    """Main ETL flow for air quality data processing"""
    data_path = get_airvisual_data_hourly()
    verified_data = read_data_airvisual(data_path)
    load_airvisual_to_postgresql(verified_data)

if __name__ == '__main__':
    air_quality_etl()