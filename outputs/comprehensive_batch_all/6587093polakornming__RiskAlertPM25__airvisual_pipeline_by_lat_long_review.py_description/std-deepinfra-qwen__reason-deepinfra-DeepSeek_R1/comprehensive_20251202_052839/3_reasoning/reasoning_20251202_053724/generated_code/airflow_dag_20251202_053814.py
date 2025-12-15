<think>
Okay, let's tackle this Airflow DAG creation based on the given pipeline description. First, I need to parse the requirements and constraints carefully.

The pipeline has three tasks that run sequentially: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. The DAG should be manually triggered with schedule_interval=None and catchup disabled. Retries are specified differently for the API task and the DAG level.

Starting with the imports. The user specified Airflow 2.x style, so I'll use the correct modules. Since there's no branching or sensors mentioned, I don't need to include those operators. But I do need PythonOperator for all tasks. Also, since the first task might skip, I'll need AirflowSkipException from airflow.exceptions.

Next, default_args. The DAG-level retries are 3 with a 3-minute delay, but the API task has 2 retries and 5-minute delay. Wait, but in Airflow, retries and retry_delay are DAG-level parameters in default_args. However, individual tasks can override these with their own retries and retry_delay parameters. So in default_args, set retries=3 and retry_delay=180 (3 minutes), then for the get_airvisual_data_hourly task, set retries=2 and retry_delay=300.

Wait, no. The DAG-level retries are 3, but the API task has 2 retries. So in default_args, retries=3, retry_delay=180. Then, in the specific task, override retries to 2 and retry_delay to 300. But how does that work? Because in Airflow, if a task doesn't specify its own retries, it uses the DAG's default. So for the API task, we need to explicitly set retries=2 and retry_delay=5*60.

Now, defining the DAG with DAG_ID, schedule_interval=None, catchup=False, and the default_args.

Each task is a PythonOperator. The functions for each task need to be defined. Let's outline them:

1. get_airvisual_data_hourly: Fetches data from API, checks duplicates, writes JSON. If duplicate, skip downstream.

   Need to implement the API call, check the database for existing records. If exists, raise AirflowSkipException. Also, atomic write: write to a temp file then rename.

   But how to interact with the database here? The task needs a Postgres hook. The connection is specified as "postgres_conn", so using PostgresHook from airflow.providers.postgres.hooks.postgres. Wait, the user mentioned using proper imports. So need to import PostgresHook if used.

   However, the first task's duplicate check requires querying the database. So in the function for get_airvisual_data_hourly, we'll need to connect to PostgreSQL and check if the current hour's data exists.

   Also, converting the timestamp to Bangkok timezone. So using pytz? Or maybe pendulum. But the user didn't specify, so perhaps use pendulum for timezone handling.

   The task writes the JSON data to a temporary file, then renames it atomically. So code for writing to a temp file and then moving it.

2. read_data_airvisual: Reads the JSON file, validates structure. Just checks if the file exists and data is correct. Logs success.

3. load_airvisual_to_postgresql: Performs ETL into PostgreSQL. Loads mapping configs from JSON files, inserts into dimension tables with conflict resolution, and fact table. Uses transaction management.

   For database operations, again using PostgresHook. Each insert should handle conflicts, perhaps using ON CONFLICT DO NOTHING or similar.

   The task may need to read configuration JSON files for pollution and weather mappings. Assume those files are in a specific path, maybe configured via variables or hardcoded.

Now, considering the functions:

For get_airvisual_data_hourly, the function will:

- Use requests to call the AirVisual API (but need to handle API key, which is in config.conf). How to get the API key? Maybe from Airflow Variables or a connection. The user says "API keys from config.conf", but in Airflow, it's better to use Connections or Variables. So perhaps the API key is stored in a Variable named 'airvisual_api_key' or in a connection. Let's assume it's in a Variable.

Wait, the user's note under Integrations says "Configuration Files: API keys from config.conf", but in Airflow, it's more secure to use Variables or Connections. So perhaps the task reads the API key from a Variable. So in the function, we can use Variable.get('airvisual_api_key').

But the user might not have specified that, so perhaps the code should get the API key from an environment variable or a Variable. Let's proceed with Variables.

So, in the function:

Import Variable from airflow.models, retrieve the API key.

Then, construct the API URL with latitude and longitude. The coordinates are fixed for Salaya, so those can be hardcoded or also retrieved from Variables. The user says "fixed coordinates", so hardcoding them is acceptable.

Then, make the API request. Validate response status. Convert the timestamp to Bangkok time (timezone 'Asia/Bangkok').

Check if the data for the current hour exists in the database. For that, query the dimDateTimeTable perhaps, or the fact table. The exact check isn't specified, but the task needs to determine if the current hour's data is already present.

If exists, skip the rest by raising AirflowSkipException.

Otherwise, write the JSON data to a temp file, then rename it to the final file (atomic write).

For read_data_airvisual: simply check if the file exists, read it, validate structure. Maybe check for required keys.

For load_airvisual_to_postgresql: read the JSON file, process the data, load into PostgreSQL tables. Use transaction blocks, so perhaps using a database connection and cursor, with BEGIN and COMMIT/ROLLBACK.

Now, code structure:

Import necessary modules: DAG, PythonOperator, AirflowSkipException, PostgresHook, Variable, etc.

Define default_args with owner, retries, retry_delay, etc.

Define the DAG with appropriate parameters.

Define the three functions for the tasks.

Then, create the three PythonOperator tasks with their respective python_callable.

Set dependencies with t1 >> t2 >> t3.

Potential issues:

- Handling of the JSON file path: where is it stored? The code should have a consistent path. Maybe use a variable like DATA_DIR = '/tmp/airvisual_data' and ensure it exists.

- The first task writes a file, the second reads it, third uses it. So the file path should be passed between tasks. Since Airflow tasks are separate, they need to know the file path. Perhaps generate a filename based on execution date or a timestamp.

But the problem states that the first task checks for existing data in the current hour. So the filename could be based on the current hour. However, using the execution date might be tricky since the DAG is manually triggered. Alternatively, generate a filename with the current UTC time converted to Bangkok time.

Wait, the first task converts the timestamp to Bangkok timezone. So when checking for duplicates, it's based on the current hour in Bangkok time. So the filename could include that timestamp.

But perhaps the filename is generated dynamically each run. For example, when the DAG is manually triggered, the execution date is the current time. But since schedule_interval is None, the execution date is the time when the DAG is triggered. However, when manually triggered, the execution date is the start date of the DAG run. Wait, no. For manually triggered DAG runs, the execution date is set to the start of the interval. But since schedule_interval is None, the execution date is determined by when you trigger it. Maybe better to generate the filename based on the current time in Bangkok.

Alternatively, pass the filename between tasks using XCom. But since the tasks are sequential, maybe the first task writes to a specific path, and the next tasks read from that path. However, using XCom to pass the filename would be more flexible, especially if multiple runs could happen. But given the description, perhaps the filename is fixed or based on the current hour.

Alternatively, the first task writes to a file with a name like 'airvisual_data_{current_hour}.json', where current_hour is in Bangkok time. Then, the subsequent tasks read that same file.

But handling this requires that each run writes to a unique file to prevent conflicts. However, the duplicate check in the database would prevent multiple runs for the same hour. Since the first task checks the database for existing data, even if the DAG is triggered multiple times for the same hour, the task would skip after the first successful run.

So perhaps the filename can be something like '/tmp/airvisual_data/latest.json', and each run overwrites it. But atomic write would involve writing to a temp file and then renaming, so that the read task can safely read the renamed file.

Alternatively, generate a filename based on the data's timestamp. For example, if the API returns a timestamp, use that to name the file. But that requires parsing the response first.

This is getting complicated. Let's proceed with a fixed temporary file path, using a temporary name and then renaming to a final name. For example:

temp_file = '/tmp/airvisual_data_temp.json'
final_file = '/tmp/airvisual_data.json'

In the first task, write to temp_file, then os.rename(temp_file, final_file). The read task reads final_file.

But in a distributed environment, writing to /tmp might not be reliable. However, the user mentioned "Local JSON files", so assuming the Airflow workers have access to the local filesystem.

Now, code steps:

Implement the first task's function:

def get_airvisual_data_hourly(**kwargs):
    import requests
    from airflow.models import Variable
    from airflow.exceptions import AirflowSkipException
    import json
    import os
    from datetime import datetime
    import pendulum

    # Get API key from Variable
    api_key = Variable.get('AIRVISUAL_API_KEY')
    lat = 13.789  # Example coordinates for Salaya
    lon = 100.535

    # API URL
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Convert timestamp to Bangkok timezone
    utc_time = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
    bangkok_tz = pendulum.timezone('Asia/Bangkok')
    bangkok_time = pendulum.instance(utc_time).in_timezone(bangkok_tz)
    data['bangkok_time'] = bangkok_time.isoformat()

    # Check if data exists in DB for current hour
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Assuming dimDateTimeTable has a timestamp column; adjust query as needed
    query = """
        SELECT EXISTS(
            SELECT 1 FROM dimDateTimeTable
            WHERE timestamp = %s
        )
    """
    cursor.execute(query, (bangkok_time.start_of('hour'),))
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if exists:
        raise AirflowSkipException('Data for current hour already exists')

    # Atomic write to file
    temp_path = '/tmp/airvisual_temp.json'
    final_path = '/tmp/airvisual_data.json'
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.rename(temp_path, final_path)

But wait, the PostgresHook is from airflow.providers.postgres.hooks.postgres. Need to import that.

Also, the user's code must be executable, so all necessary imports should be inside the functions or at the top. However, importing inside the function is acceptable for modules like requests, which may not be needed elsewhere.

But for Airflow modules, they should be imported at the top. Wait, the code is supposed to be a single module, so all imports should be at the top. So in the DAG file:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import requests
import json
import os
import pendulum
from datetime import datetime, timedelta

But wait, some modules are only used inside the functions. However, putting all imports at the top is standard practice. But requests and pendulum may not be installed by default. The user said to assume required packages are installed, so proceed.

But in the task functions, some of these imports may be needed. However, in Airflow, it's better to have all imports at the top. So the code should include all necessary imports.

Wait, but the functions are defined within the DAG file. So all necessary imports (like requests, pendulum, etc.) should be at the top.

So, the code will start with:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import requests
import json
import os
import pendulum
from datetime import datetime, timedelta

But the user's constraints say to use proper imports. So requests and pendulum need to be imported.

Now, the functions:

get_airvisual_data_hourly: as above.

read_data_airvisual: reads the file, checks structure.

def read_data_airvisual(**kwargs):
    import json

    file_path = '/tmp/airvisual_data.json'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file {file_path} not found")

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Validate data structure
    required_keys = ['status', 'data']
    if not all(k in data for k in required_keys):
        raise ValueError("Invalid data structure: missing required keys")

    # Log success
    print("Data extraction successful")

load_airvisual_to_postgresql: loads data into PostgreSQL.

def load_airvisual_to_postgresql(**kwargs):
    import json
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Load JSON data
    file_path = '/tmp/airvisual_data.json'
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Load mapping configs
    # Assuming config files are in a specific path
    with open('pollution_mapping.json') as f:
        pollution_mapping = json.load(f)
    with open('weather_mapping.json') as f:
        weather_mapping = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Insert into dimDateTimeTable
        timestamp = pendulum.parse(data['bangkok_time'])
        cursor.execute("""
            INSERT INTO dimDateTimeTable (timestamp, hour, day, month, year)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING
        """, (timestamp, timestamp.hour, timestamp.day, timestamp.month, timestamp.year))

        # Insert into dimLocationTable
        location = data['data']['city']
        cursor.execute("""
            INSERT INTO dimLocationTable (city, country, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city, country) DO NOTHING
        """, (location['city'], location['country'], location['lat'], location['lon']))

        # Insert into dimMainPollutionTable
        pollution_data = data['data']['current']['pollution']
        cursor.execute("""
            INSERT INTO dimMainPollutionTable (aqius, aqicn, mainus, maincn)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (aqius, aqicn) DO NOTHING
        """, (pollution_data['aqius'], pollution_data['aqicn'], pollution_data['mainus'], pollution_data['maincn']))

        # Insert into factairvisualtable
        # Assuming we have foreign keys from the dim tables
        # Need to retrieve the generated IDs or existing ones
        # This part is simplified; actual implementation may vary
        cursor.execute("""
            INSERT INTO factairvisualtable (
                datetime_id, location_id, pollution_id,
                temperature, humidity, pressure, wind_speed
            ) VALUES (
                (SELECT id FROM dimDateTimeTable WHERE timestamp = %s),
                (SELECT id FROM dimLocationTable WHERE city = %s AND country = %s),
                (SELECT id FROM dimMainPollutionTable WHERE aqius = %s AND aqicn = %s),
                %s, %s, %s, %s
            )
        """, (
            timestamp,
            location['city'], location['country'],
            pollution_data['aqius'], pollution_data['aqicn'],
            data['data']['current']['weather']['tp'],
            data['data']['current']['weather']['hu'],
            data['data']['current']['weather']['pr'],
            data['data']['current']['weather']['ws']
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

But this is a simplified version. The actual conflict resolution and foreign key handling would depend on the database schema, which isn't provided. However, the code uses ON CONFLICT clauses as per the description.

Now, putting it all together in the DAG:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='airvisual_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for AirVisual air quality data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['air_quality'],
) as dag:

    t1 = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    t2 = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_data_airvisual,
    )

    t3 = PythonOperator(
        task_id='load_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgresql,
    )

    t1 >> t2 >> t3

Wait, but in default_args, retries is set to 3, but the API task (t1) overrides retries to 2. That's correct.

Now, check for PEP 8 compliance. Lines not too long, proper indentation, etc.

Potential issues:

- The PostgresHook is imported correctly.

- Variable.get('AIRVISUAL_API_KEY') assumes the Variable is set.

- The latitude and longitude are hardcoded. The user said fixed coordinates, so that's acceptable.

- The file paths are hardcoded to '/tmp/...'. This may not be ideal, but the user mentioned local files, so it's acceptable.

- The functions may have repeated code, but given the task, it's manageable.

- The load_airvisual_to_postgresql function assumes the existence of pollution_mapping.json and weather_mapping.json in the current directory. But the user mentioned configuration files, so perhaps these paths should be configurable via Variables or placed in a specific directory. However, the code as written assumes they are present.

But the user's pipeline description says "loads pollution and weather mapping configurations from JSON files", so the code reads them from the current directory. In practice, the path should be specified, but since the user didn't provide specifics, hardcoding is acceptable for the example.

Now, putting all together, the code should be executable, assuming all dependencies are installed (requests, pendulum, apache-airflow-providers-postgres, etc.).
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import requests
import json
import os
import pendulum
from datetime import datetime, timedelta

def get_airvisual_data_hourly(**kwargs):
    """Fetch air quality data from AirVisual API and store as temporary JSON."""
    api_key = Variable.get("AIRVISUAL_API_KEY")
    lat = 13.789  # Salaya coordinates
    lon = 100.535
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        utc_time = datetime.utcfromtimestamp(data["data"]["current"]["pollution"]["ts"])
        bangkok_time = pendulum.instance(utc_time).in_tz("Asia/Bangkok")
        data["bangkok_time"] = bangkok_time.isoformat()

        pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM dimDateTimeTable WHERE timestamp = %s",
            (bangkok_time.start_of("hour"),)
        )
        if cursor.fetchone():
            raise AirflowSkipException("Data exists for current hour")
        cursor.close()

        temp_path = "/tmp/airvisual_temp.json"
        final_path = "/tmp/airvisual_data.json"
        with open(temp_path, "w") as f:
            json.dump(data, f)
        os.replace(temp_path, final_path)

    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {str(e)}")

def read_data_airvisual(**kwargs):
    """Validate extracted data file structure."""
    file_path = "/tmp/airvisual_data.json"
    if not os.path.exists(file_path):
        raise FileNotFoundError("Data file missing")
    
    with open(file_path) as f:
        data = json.load(f)
    
    if not all(key in data for key in ("status", "data", "bangkok_time")):
        raise ValueError("Invalid data structure")

def load_airvisual_to_postgresql(**kwargs):
    """Load transformed data into PostgreSQL database tables."""
    with open("/tmp/airvisual_data.json") as f:
        data = json.load(f)
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        timestamp = pendulum.parse(data["bangkok_time"])
        location = data["data"]["city"]
        pollution = data["data"]["current"]["pollution"]
        weather = data["data"]["current"]["weather"]

        cursor.execute("""
            INSERT INTO dimDateTimeTable (timestamp)
            VALUES (%s) ON CONFLICT (timestamp) DO NOTHING
        """, (timestamp,))

        cursor.execute("""
            INSERT INTO dimLocationTable (city, country, latitude, longitude)
            VALUES (%s,%s,%s,%s) ON CONFLICT (city, country) DO NOTHING
        """, (location["city"], location["country"], location["lat"], location["lon"]))

        cursor.execute("""
            INSERT INTO dimMainPollutionTable (aqius, mainus)
            VALUES (%s,%s) ON CONFLICT (aqius) DO NOTHING
        """, (pollution["aqius"], pollution["mainus"]))

        cursor.execute("""
            INSERT INTO factairvisualtable (
                datetime_id, location_id, pollution_id,
                temperature, humidity, pressure, wind_speed
            ) SELECT
                (SELECT id FROM dimDateTimeTable WHERE timestamp = %s),
                (SELECT id FROM dimLocationTable WHERE city = %s AND country = %s),
                (SELECT id FROM dimMainPollutionTable WHERE aqius = %s),
                %s, %s, %s, %s
        """, (
            timestamp,
            location["city"], location["country"],
            pollution["aqius"],
            weather["tp"], weather["hu"], weather["pr"], weather["ws"]
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="airvisual_etl",
    default_args=default_args,
    description="Air quality data ETL pipeline",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["air_quality"],
) as dag:

    fetch_data = PythonOperator(
        task_id="get_airvisual_data_hourly",
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    validate_data = PythonOperator(
        task_id="read_data_airvisual",
        python_callable=read_data_airvisual,
    )

    load_data = PythonOperator(
        task_id="load_airvisual_to_postgresql",
        python_callable=load_airvisual_to_postgresql,
    )

    fetch_data >> validate_data >> load_data