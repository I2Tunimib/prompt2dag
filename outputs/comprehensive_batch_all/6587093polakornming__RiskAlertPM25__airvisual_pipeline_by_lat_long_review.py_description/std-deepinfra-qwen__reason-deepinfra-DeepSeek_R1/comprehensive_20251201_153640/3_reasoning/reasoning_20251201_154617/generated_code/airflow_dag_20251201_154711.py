<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has three tasks that run sequentially: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. The dependencies are linear, so t1 → t2 → t3. The schedule is manual, so schedule_interval should be None, and catchup needs to be disabled.

Looking at the Airflow constraints, I should use Airflow 2.x style imports. So I'll import DAG from airflow, and PythonOperator from airflow.operators.python. Since there are no sensors or branching mentioned, I don't need to include those operators. The tasks are all Python functions, so PythonOperator is appropriate.

Default_args should include retries and retry_delay as specified. The get_airvisual_data_hourly task has 2 retries with 5-minute delay, while the DAG-level retries are 3 minutes. Wait, the description says "2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". Hmm, maybe the DAG's default_args retries is 2, but the retry_delay is 3 minutes? Or does the API task override the retry_delay? The problem says for the API task, retries are 2 with 5-minute delay, and DAG-level (which would be the default) has 3-minute delay. So perhaps the DAG's default_args has retries=2, retry_delay=3 minutes, but the API task has its own retry_delay set. Wait, but in Airflow, the task-level retry_delay overrides the default_args. So maybe in default_args, retries is 2, retry_delay is 3 minutes, but the API task has retries=2 and retry_delay=5*60? Or maybe the DAG-level retries are 3? Wait, the description says "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So DAG-level retries (default_args) would have retry_delay=3 minutes, but the API task has retries=2 and retry_delay=5 minutes. Wait, no—retries are the number of retries. The DAG-level retries would be the default number of retries for all tasks unless overridden. But the description says for the API task, 2 retries with 5-minute delay. For other tasks, perhaps the DAG-level retries (which is 3?) but the user says "DAG-level retries" as 3-minute delay. Wait, maybe the DAG's default_args has retries=2 (for API task) but other tasks have retries=3? Or perhaps the DAG-level retries are 3, but the API task overrides to 2. Wait, the description says: "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So maybe the DAG's default_args has retries=2, retry_delay=5 minutes, but then the other tasks would inherit that. But the DAG-level retries are 3-minute delay. Wait, perhaps the user made a mistake here. Let me recheck the original description:

"Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries"

Hmm, maybe the DAG-level retries (i.e., default_args) have retries=3, but the API task overrides to retries=2 and retry_delay=5 minutes. But that doesn't align. Alternatively, perhaps the DAG's default_args has retries=3 (the DAG-level retries) with retry_delay=3 minutes, and the API task has retries=2 and retry_delay=5*60. That makes sense. So in default_args, retries=3, retry_delay=180 (3 minutes), but the API task will have its own retries and retry_delay. Wait, but the user says "2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So DAG-level retries (the default for all tasks) have retry_delay=3 minutes, but the API task has retries=2 and retry_delay=5 minutes. So in the default_args, retries=2? Or retries=3? Wait, the description says "DAG-level retries" have 3-minute delay. So perhaps the DAG's default_args has retries=3 (number) and retry_delay=3 minutes. But the API task has retries=2 and retry_delay=5 minutes. But how to set that? Because in Airflow, the task's parameters override the default_args. So in the PythonOperator for the API task, we can specify retries=2 and retry_delay=5*60. But the other tasks (read and load) would use the DAG's default_args. Wait, but the description says "DAG-level retries" have 3-minute delay. So perhaps the default_args for the DAG has retries=3 (number of retries) and retry_delay=3 minutes. Then, the API task overrides retries=2 and retry_delay=5*60. That way, the API task has 2 retries with 5 minutes delay, and other tasks (if they fail) would have 3 retries with 3 minutes delay. But according to the pipeline steps, the other tasks (read and load) don't mention retries, so perhaps they use the DAG-level retries. But the problem says "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So DAG-level retries (the default) have retry_delay=3 minutes. So default_args should have retry_delay=180 (3 minutes). The number of retries for the DAG-level is perhaps 3? But the problem says for the API task, retries=2. So maybe the DAG's default_args has retries=3, retry_delay=180, and the API task overrides retries=2 and retry_delay=300. That seems plausible.

So default_args would be:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

But the API task needs retries=2 and retry_delay=5 minutes. So in the PythonOperator for get_airvisual_data_hourly, set retries=2 and retry_delay=timedelta(minutes=5).

Wait, but the problem says "DAG-level retries" have 3-minute delay. So maybe the DAG's default_args has retries=2 (since the API task is part of the DAG and perhaps the DAG-level refers to the default retries?), but that's unclear. Alternatively, perhaps the DAG's retries are 3, but the API task overrides to 2. Let me proceed with that.

Next, the tasks:

The first task, get_airvisual_data_hourly, needs to fetch data from the AirVisual API. It checks for duplicates in the database and skips downstream processing if data exists. So in the function for this task, after fetching the data, it checks the database. If the data exists, it raises AirflowSkipException, which skips this task and downstream tasks.

The function should write the JSON data to a temporary file, then commit it (atomic write). So perhaps using a temporary file and then moving it to the final location.

The second task, read_data_airvisual, reads the JSON file to verify extraction success. It checks file accessibility and structure.

The third task, load_airvisual_to_postgresql, performs ETL into PostgreSQL. It uses the connection "postgres_conn", which should be set up in Airflow connections. The task loads mapping configs from JSON files, inserts into dimension tables with conflict resolution, and the fact table. Uses transaction management.

Now, for the functions:

Each task corresponds to a Python function. The first task's function will need access to the AirVisual API, using an API key from config.conf. But in Airflow, it's better to use Variables or Connections for secrets. However, the problem states that API keys are from config.conf. But in practice, it's better to use Airflow's Variables or Secrets. However, since the user's description mentions config.conf, perhaps the code reads from a configuration file. But for the sake of the example, maybe the function uses a placeholder for the API key, or retrieves it via a variable. However, the code needs to be executable, so perhaps the functions have placeholders, but with comments indicating where to get the API key.

But the code should be minimal and safe. So perhaps the function for get_airvisual_data_hourly has a placeholder for the API key, but in reality, it would be retrieved via an environment variable or Airflow Variable. But the code must be executable, so maybe the user is expected to have the API key in a Variable named 'airvisual_api_key' or similar.

Similarly, the PostgreSQL connection is specified as "postgres_conn", which should be defined in Airflow connections.

Now, writing the functions:

For get_airvisual_data_hourly:

- Use requests to call the AirVisual API with latitude/longitude.
- Convert the timestamp to Bangkok timezone.
- Check the database for existing records for the current hour.
- If exists, raise AirflowSkipException.
- Write the JSON data atomically (e.g., write to a temp file, then rename).

But how to check the database? The function would need to connect to PostgreSQL. So using PostgresHook from Airflow.

Wait, the task is supposed to check if the data already exists in the database for the current hour. So perhaps the function connects to the PostgreSQL database using the PostgresHook, checks the relevant tables (maybe factairvisualtable?) for a record with the current hour's timestamp.

But the exact SQL query isn't specified. So in the code, I'll need to write a placeholder SQL query, like checking dimDateTimeTable for the existence of the timestamp.

But for the code to be executable, perhaps the function has a dummy check. Alternatively, assume that the database has a table and the function checks if the timestamp exists.

Similarly, the function for loading data into PostgreSQL will need to perform multiple inserts, using transaction blocks. Using the PostgresHook again, with autocommit off, and handling transactions.

Now, putting it all together.

Imports needed:

- from airflow import DAG
- from airflow.operators.python import PythonOperator
- from airflow.exceptions import AirflowSkipException
- from airflow.providers.postgres.hooks.postgres import PostgresHook
- from datetime import datetime, timedelta
- import json
- import requests
- from tempfile import NamedTemporaryFile
- import os

But need to make sure that the code uses proper imports. For example, PostgresHook is from airflow.providers.postgres.hooks.postgres.

The DAG is defined with default_args, schedule_interval=None, catchup=False.

The functions:

def get_airvisual_data_hourly(**kwargs):
    # Fetch API key from config (in reality, use Airflow Variables or Secrets)
    api_key = "your_api_key"  # Should be retrieved from config.conf or Airflow Variables
    lat = 13.789  # Example coordinates for Salaya
    lon = 100.501
    
    # API request
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    # Assuming the API returns UTC timestamp, convert to Asia/Bangkok
    # Implementation details may vary based on API response structure
    # Placeholder for timestamp conversion
    ts = data.get('data', {}).get('current', {}).get('pollution', {}).get('ts')
    if not ts:
        raise ValueError("No timestamp in API response")
    # Convert ts to datetime and adjust timezone
    # This is a placeholder; actual conversion needed
    dt_utc = datetime.utcfromtimestamp(ts)
    dt_bangkok = dt_utc.astimezone(timezone('Asia/Bangkok'))
    
    # Check database for existing data
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM dimDateTimeTable WHERE datetime = %s",
        (dt_bangkok.replace(minute=0, second=0, microsecond=0),)
    )
    if cursor.fetchone():
        cursor.close()
        conn.close()
        raise AirflowSkipException("Data already exists for current hour")
    
    # Atomic write to JSON file
    temp_path = "/tmp/airvisual_data_temp.json"
    final_path = "/tmp/airvisual_data.json"
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.rename(temp_path, final_path)
    
    cursor.close()
    conn.close()

def read_data_airvisual(**kwargs):
    file_path = "/tmp/airvisual_data.json"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Validate data structure
    required_keys = ['data', 'status']
    if not all(key in data for key in required_keys):
        raise ValueError("Invalid data structure")
    
    # Log success
    print("Data extraction verified successfully")

def load_airvisual_to_postgresql(**kwargs):
    file_path = "/tmp/airvisual_data.json"
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Load mapping configurations (placeholder paths)
        with open('pollution_mapping.json') as f:
            pollution_map = json.load(f)
        with open('weather_mapping.json') as f:
            weather_map = json.load(f)
        
        # Extract timestamp and convert to Bangkok timezone
        ts = data['data']['current']['pollution']['ts']
        dt_utc = datetime.utcfromtimestamp(ts)
        dt_bangkok = dt_utc.astimezone(timezone('Asia/Bangkok'))
        hour_truncated = dt_bangkok.replace(minute=0, second=0, microsecond=0)
        
        # Insert into dimDateTimeTable
        cursor.execute(
            """INSERT INTO dimDateTimeTable (datetime)
               VALUES (%s) ON CONFLICT (datetime) DO NOTHING""",
            (hour_truncated,)
        )
        
        # Insert into dimLocationTable (example fields)
        location_data = data['data']['city']
        cursor.execute(
            """INSERT INTO dimLocationTable (city, country, latitude, longitude)
               VALUES (%s, %s, %s, %s) ON CONFLICT (latitude, longitude) DO NOTHING""",
            (location_data['city'], location_data['country'], location_data['lat'], location_data['lon'])
        )
        
        # Insert into dimMainPollutionTable
        pollution_data = data['data']['current']['pollution']
        cursor.execute(
            """INSERT INTO dimMainPollutionTable (aqius, mainus, aqicn, maincn)
               VALUES (%s, %s, %s, %s) ON CONFLICT (aqius, aqicn) DO NOTHING""",
            (pollution_data['aqius'], pollution_data['mainus'], pollution_data['aqicn'], pollution_data['maincn'])
        )
        
        # Insert into factairvisualtable
        # Assuming some structure; adjust based on actual table schema
        weather_data = data['data']['current']['weather']
        cursor.execute(
            """INSERT INTO factairvisualtable
               (datetime_id, location_id, pollution_id, temperature, humidity, windspeed)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (hour_truncated,  # Assuming datetime_id is the truncated hour
             1,  # Placeholder for location_id (should retrieve from insert)
             1,  # Placeholder for pollution_id
             weather_data['tp'], weather_data['hu'], weather_data['ws'])
        )
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

Now, putting all this into the DAG structure.

But wait, the functions use 'timezone' from datetime, which requires importing pytz or using zoneinfo in Python 3.9+. So need to import timezone from datetime, but in Python 3.9+, it's from zoneinfo. Alternatively, use pytz. But to keep it simple, perhaps use pytz. So add 'import pytz' and adjust the timezone conversion.

Wait, the user's code needs to be executable. So perhaps the functions should handle timezones properly. Let me adjust:

In the get_airvisual_data_hourly function:

from datetime import datetime, timedelta
import pytz

dt_utc = datetime.utcfromtimestamp(ts)
dt_utc = dt_utc.replace(tzinfo=pytz.utc)
dt_bangkok = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))

Similarly in the load function.

But need to make sure that pytz is imported.

But the code must be executable. So adding 'import pytz' at the top.

Now, putting all together.

The DAG code:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pytz
import json
import requests
import os

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
    tags=['airvisual', 'etl'],
) as dag:

    def get_airvisual_data_hourly(**kwargs):
        api_key = "your_api_key"  # Should be from config.conf or Airflow Variables
        lat = 13.789
        lon = 100.501
        
        url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        ts = data.get('data', {}).get('current', {}).get('pollution', {}).get('ts')
        if not ts:
            raise ValueError("No timestamp in API response")
        dt_utc = datetime.utcfromtimestamp(ts)
        dt_utc = dt_utc.replace(tzinfo=pytz.utc)
        dt_bangkok = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
        hour_truncated = dt_bangkok.replace(minute=0, second=0, microsecond=0)
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM dimDateTimeTable WHERE datetime = %s",
            (hour_truncated,)
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise AirflowSkipException("Data exists, skipping")
        
        temp_path = "/tmp/airvisual_data_temp.json"
        final_path = "/tmp/airvisual_data.json"
        with open(temp_path, 'w') as f:
            json.dump(data, f)
        os.rename(temp_path, final_path)
        
        cursor.close()
        conn.close()

    def read_data_airvisual(**kwargs):
        file_path = "/tmp/airvisual_data.json"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if 'data' not in data or 'status' not in data:
            raise ValueError("Invalid data structure")
        
        print("Data verified successfully")

    def load_airvisual_to_postgresql(**kwargs):
        file_path = "/tmp/airvisual_data.json"
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Load mappings (example paths)
            with open('pollution_mapping.json') as f:
                pollution_map = json.load(f)
            with open('weather_mapping.json') as f:
                weather_map = json.load(f)
            
            ts = data['data']['current']['pollution']['ts']
            dt_utc = datetime.utcfromtimestamp(ts)
            dt_utc = dt_utc.replace(tzinfo=pytz.utc)
            dt_bangkok = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
            hour_truncated = dt_bangkok.replace(minute=0, second=0, microsecond=0)
            
            # Insert into dimDateTimeTable
            cursor.execute(
                """INSERT INTO dimDateTimeTable (datetime)
                   VALUES (%s) ON CONFLICT (datetime) DO NOTHING""",
                (hour_truncated,)
            )
            
            # Insert location
            location = data['data']['city']
            cursor.execute(
                """INSERT INTO dimLocationTable (city, country, latitude, longitude)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (latitude, longitude) DO NOTHING""",
                (location['city'], location['country'], location['lat'], location['lon'])
            )
            
            # Insert pollution metadata
            pollution = data['data']['current']['pollution']
            cursor.execute(
                """INSERT INTO dimMainPollutionTable (aqius, mainus, aqicn, maincn)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (aqius, aqicn) DO NOTHING""",
                (pollution['aqius'], pollution['mainus'], pollution['aqicn'], pollution['maincn'])
            )
            
            # Insert fact data
            weather = data['data']['current']['weather']
            cursor.execute(
                """INSERT INTO factairvisualtable
                   (datetime_id, location_id, pollution_id, temperature, humidity, windspeed)
                   VALUES (%s, (SELECT id FROM dimLocationTable WHERE latitude = %s AND longitude = %s),
                           (SELECT id FROM dimMainPollutionTable WHERE aqius = %s AND aqicn = %s),
                           %s, %s, %s)""",
                (hour_truncated, location['lat'], location['lon'],
                 pollution['aqius'], pollution['aqicn'],
                 weather['tp'], weather['hu'], weather['ws'])
            )
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

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
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import requests
import os
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

def get_airvisual_data_hourly(**kwargs):
    config = {
        'api_key': os.getenv('AIRVISUAL_API_KEY', 'default_key'),
        'lat': 13.789,
        'lon': 100.501,
        'output_path': '/tmp/airvisual_data.json'
    }
    
    try:
        response = requests.get(
            f"http://api.airvisual.com/v2/nearest_city?lat={config['lat']}"
            f"&lon={config['lon']}&key={config['api_key']}"
        )
        response.raise_for_status()
        data = response.json()

        ts = data['data']['current']['pollution']['ts']
        dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)
        dt_bkk = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
        hour_start = dt_bkk.replace(minute=0, second=0, microsecond=0)

        pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dimDateTimeTable WHERE datetime = %s", (hour_start,))
            if cur.fetchone():
                raise AirflowSkipException(f"Data exists for {hour_start.isoformat()}")

        tmp_path = f"{config['output_path']}.tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f)
        os.rename(tmp_path, config['output_path'])

    except requests.exceptions.HTTPError as e:
        raise AirflowSkipException(f"API request failed: {str(e)}")

def read_data_airvisual(**kwargs):
    file_path = '/tmp/airvisual_data.json'
    if not os.path.exists(file_path):
        raise FileNotFoundError("No data file available for processing")

    with open(file_path) as f:
        data = json.load(f)
    
    if not all(k in data for k in ('data', 'status')):
        raise ValueError("Invalid data structure from source file")

def load_airvisual_to_postgresql(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()

    with open('/tmp/airvisual_data.json') as f:
        data = json.load(f)
    
    try:
        with conn.cursor() as cur:
            ts = data['data']['current']['pollution']['ts']
            dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)
            dt_bkk = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
            hour_start = dt_bkk.replace(minute=0, second=0, microsecond=0)

            cur.execute("""
                INSERT INTO dimDateTimeTable (datetime)
                VALUES (%s) ON CONFLICT (datetime) DO NOTHING
            """, (hour_start,))

            loc = data['data']['city']
            cur.execute("""
                INSERT INTO dimLocationTable (city, country, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (latitude, longitude) DO NOTHING
            """, (loc['city'], loc['country'], loc['lat'], loc['lon']))

            pol = data['data']['current']['pollution']
            cur.execute("""
                INSERT INTO dimMainPollutionTable (aqius, mainus, aqicn, maincn)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (aqius, aqicn) DO NOTHING
            """, (pol['aqius'], pol['mainus'], pol['aqicn'], pol['maincn']))

            weather = data['data']['current']['weather']
            cur.execute("""
                INSERT INTO factairvisualtable 
                (datetime_id, location_id, pollution_id, temperature, humidity, windspeed)
                VALUES (
                    (SELECT id FROM dimDateTimeTable WHERE datetime = %s),
                    (SELECT id FROM dimLocationTable WHERE latitude = %s AND longitude = %s),
                    (SELECT id FROM dimMainPollutionTable WHERE aqius = %s AND aqicn = %s),
                    %s, %s, %s
                )
            """, (hour_start, loc['lat'], loc['lon'], 
                  pol['aqius'], pol['aqicn'],
                  weather['tp'], weather['hu'], weather['ws']))
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

with DAG(
    dag_id='airquality_etl',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['airquality'],
) as dag:

    fetch_task = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    validate_task = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_data_airvisual
    )

    load_task = PythonOperator(
        task_id='load_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgresql
    )

    fetch_task >> validate_task >> load_task