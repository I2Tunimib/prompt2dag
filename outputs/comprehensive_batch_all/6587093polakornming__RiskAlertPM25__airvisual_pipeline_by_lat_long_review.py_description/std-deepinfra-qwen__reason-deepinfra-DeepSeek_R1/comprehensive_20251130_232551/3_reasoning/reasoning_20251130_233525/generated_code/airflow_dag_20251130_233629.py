<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has three tasks that must run sequentially: get_airvisual_data_hourly → read_data_airvisual → load_airvisual_to_postgresql. The DAG should have a manual schedule (schedule_interval=None), catchup disabled, and specific retry configurations. The tasks involve API calls, file operations, and database interactions.

Starting with the DAG definition. The default_args should include retries, retry_delay, and other common parameters. The user mentioned retry configurations: 2 retries with 5-minute delay for the API task, but DAG-level retries have 3-minute delay. Wait, the DAG-level retries would be the default_args. So default_args retries is 3, retry_delay is 3 minutes? Or maybe the user meant that the API task has task-specific retries. Hmm, the description says "Retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries". So DAG-level (default) retries are 3 retries with 3 minutes delay, but the API task overrides that with 2 retries and 5 minutes. So in default_args, set 'retries': 3 and 'retry_delay': timedelta(minutes=3). Then, in the PythonOperator for get_airvisual_data_hourly, specify retries=2 and retry_delay=5 minutes. But wait, in Airflow, task-specific retry parameters override the default_args. So I need to set that in the task.

Next, the tasks. Each task is a PythonOperator, except if there are sensors or branches, but the description says no sensors or branches. So all are PythonOperators.

For the functions:

get_airvisual_data_hourly needs to fetch data from AirVisual API, check duplicates in DB, save JSON temp file. It should raise AirflowSkipException if data exists. So the function will use the Postgres hook to check for existing records. Also, it uses an API key from config.conf. Wait, how to handle the configuration? Since the user mentioned config.conf, maybe they're using Airflow Variables or a separate file. But in the code, I can't read from a local file unless it's in the Airflow context. So perhaps the API key is stored in Airflow Variables. So in the code, I'll use Variable.get('AIRVISUAL_API_KEY') and Variable.get('AIRVISUAL_COORDINATES') or something. Alternatively, maybe they're using a Connection. But the problem states to assume required packages are installed, so perhaps the API key is handled via a Connection or Variables. For simplicity, I'll use Variables.

The function will need to access the Postgres database to check for existing data. The Postgres connection is specified as "postgres_conn", so the hook should use that connection ID.

Then, the task writes the JSON data to a temporary file atomically. So perhaps using a temporary file and then renaming it.

The next task, read_data_airvisual, reads the saved JSON file to verify. It checks accessibility and structure.

The third task, load_airvisual_to_postgresql, performs ETL into PostgreSQL. It loads mapping configs from JSON files (maybe stored in the Airflow environment), and inserts into various tables with conflict resolution. This will involve multiple SQL statements, possibly using PostgresHook to run queries.

Now, handling the functions:

For get_airvisual_data_hourly:

- Fetch data from API using requests.get, with the API key and coordinates.
- Convert timestamp to Bangkok timezone.
- Check if the data for the current hour exists in the database. How? The timestamp in the data is converted to Bangkok time, then check if that hour's data is already present in the fact table.
- If exists, raise AirflowSkipException.
- Otherwise, write the JSON data to a temp file, then atomically rename it.

But how to get the current hour? Maybe the API response includes a timestamp, which is converted to Bangkok time. Then extract the hour from that timestamp and check if there's an entry in the database for that hour.

Wait, the task is named 'hourly', so perhaps it's meant to run hourly, but the DAG is manually triggered. So when manually triggered, it checks for the current hour's data.

But the exact implementation details might vary. For the code, I'll need to write a function that does these steps.

For the database check, using PostgresHook:

hook = PostgresHook(postgres_conn_id='postgres_conn')
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM factairvisualtable WHERE datetime = %s", (datetime_value,))
count = cursor.fetchone()[0]
if count > 0:
    raise AirflowSkipException("Data already exists")

But the exact query depends on the schema. The problem states that the task checks if data exists for the current hour. So perhaps the datetime is stored in the database, and the check is whether there's a record with the same datetime as the one retrieved from the API.

The function then saves the JSON data to a temporary file. Using a temporary file and then renaming ensures atomicity. For example, write to a .tmp file and then rename to .json.

For read_data_airvisual, the function checks if the file exists and reads it to validate structure.

For load_airvisual_to_postgresql, the function reads the JSON file, loads mapping configurations (pollution and weather mappings from JSON files), and performs inserts into the dimension and fact tables with conflict resolution (e.g., ON CONFLICT DO NOTHING).

The mapping files are probably in the Airflow DAG folder or a specific directory. The code can open them using standard file operations, assuming they're accessible.

Now, putting it all together.

Imports needed:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

import json
from datetime import datetime, timedelta
import os
import requests
import pytz

Wait, in Airflow 2.x, the PostgresHook is in airflow.providers.postgres.hooks.postgres. So the correct import would be:

from airflow.providers.postgres.hooks.postgres import PostgresHook

But the user specified to use Airflow 2.x style imports. So I need to make sure the imports are correct.

Yes, for Airflow 2.x, the PostgresHook is from airflow.providers.postgres.hooks.postgres.

So the correct import is:

from airflow.providers.postgres.operators.postgres import PostgresOperator  # Not needed here
from airflow.providers.postgres.hooks.postgres import PostgresHook

But in the code, the user may not need PostgresOperator since they're using PythonOperator.

Now, the default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

But the API task (get_airvisual_data_hourly) needs 2 retries with 5-minute delay. So in the PythonOperator for that task, we can override the retries and retry_delay parameters.

Wait, but in Airflow, the task's retries and retry_delay are set in the operator arguments. So when defining the task, we can set retries=2 and retry_delay=timedelta(minutes=5).

So the DAG-level default_args has retries=3 and retry_delay=3min, but the API task will have retries=2 and retry_delay=5min.

Now, defining the DAG:

with DAG(
    dag_id='airvisual_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['air_quality', 'etl'],
) as dag:

Then the three tasks.

For the first task:

def get_airvisual_data_hourly(**kwargs):
    # Fetch API key from Airflow Variables
    api_key = Variable.get("AIRVISUAL_API_KEY")
    coordinates = Variable.get("AIRVISUAL_COORDINATES")  # Assuming format like '13.7891,100.5353'
    lat, lon = coordinates.split(',')
    
    # API request
    url = f"https://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    utc_time = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
    bangkok_tz = pytz.timezone('Asia/Bangkok')
    local_time = utc_time.astimezone(bangkok_tz)
    data['data']['current']['pollution']['ts'] = local_time.isoformat()
    
    # Check for existing data in PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    datetime_value = local_time.replace(minute=0, second=0, microsecond=0)
    cursor.execute(
        "SELECT COUNT(*) FROM factairvisualtable WHERE datetime = %s",
        (datetime_value,)
    )
    if cursor.fetchone()[0] > 0:
        raise AirflowSkipException(f"Data for {datetime_value} already exists")
    
    # Atomic write to temporary file
    tmp_path = '/tmp/airvisual_data.tmp'  # Adjust path as needed
    final_path = '/tmp/airvisual_data.json'
    with open(tmp_path, 'w') as f:
        json.dump(data, f)
    os.rename(tmp_path, final_path)

But wait, the user mentioned 'implements atomic file write pattern to save JSON data temporarily before committing'. So writing to a temp file and then renaming it, which is atomic.

Then, the PythonOperator for this task:

get_airvisual_data = PythonOperator(
    task_id='get_airvisual_data_hourly',
    python_callable=get_airvisual_data_hourly,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

Next task: read_data_airvisual.

def read_data_airvisual(**kwargs):
    file_path = '/tmp/airvisual_data.json'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Validate data structure
    required_keys = ['status', 'data']
    if not all(key in data for key in required_keys):
        raise ValueError("Invalid data structure: missing required keys")
    
    # Log success
    print("Data extraction verified successfully")

read_data = PythonOperator(
    task_id='read_data_airvisual',
    python_callable=read_data_airvisual,
)

Third task: load_airvisual_to_postgresql.

def load_airvisual_to_postgresql(**kwargs):
    # Load JSON data
    file_path = '/tmp/airvisual_data.json'
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Load mapping configurations
    pollution_mapping_path = 'pollution_mapping.json'
    weather_mapping_path = 'weather_mapping.json'
    
    with open(pollution_mapping_path, 'r') as f:
        pollution_mapping = json.load(f)
    
    with open(weather_mapping_path, 'r') as f:
        weather_mapping = json.load(f)
    
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Insert into dimDateTimeTable
        datetime_value = datetime.fromisoformat(data['data']['current']['pollution']['ts'])
        cursor.execute(
            """
            INSERT INTO dimDateTimeTable (datetime, timestamp, year, month, day, hour)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (datetime) DO NOTHING
            """,
            (
                datetime_value,
                datetime_value,
                datetime_value.year,
                datetime_value.month,
                datetime_value.day,
                datetime_value.hour,
            )
        )
        
        # Insert into dimLocationTable
        location_data = data['data']['city']
        cursor.execute(
            """
            INSERT INTO dimLocationTable (city, country, state)
            VALUES (%s, %s, %s)
            ON CONFLICT (city, country, state) DO NOTHING
            """,
            (location_data['city'], location_data['country'], location_data['state'])
        )
        
        # Insert into dimMainPollutionTable
        pollution_data = data['data']['current']['pollution']
        cursor.execute(
            """
            INSERT INTO dimMainPollutionTable (aqius, aqicn, mainus, maincn)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (aqius, aqicn, mainus, maincn) DO NOTHING
            """,
            (
                pollution_data['aqius'],
                pollution_data['aqicn'],
                pollution_data['mainus'],
                pollution_data['maincn'],
            )
        )
        
        # Insert into factairvisualtable
        weather_data = data['data']['current']['weather']
        cursor.execute(
            """
            INSERT INTO factairvisualtable (
                datetime, location_id, pollution_id,
                temperature, humidity, windspeed, pressure,
                aqius, aqicn, mainus, maincn
            )
            VALUES (
                %s, 
                (SELECT id FROM dimLocationTable WHERE city=%s AND country=%s AND state=%s),
                (SELECT id FROM dimMainPollutionTable WHERE aqius=%s AND aqicn=%s AND mainus=%s AND maincn=%s),
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                datetime_value,
                location_data['city'], location_data['country'], location_data['state'],
                pollution_data['aqius'], pollution_data['aqicn'], pollution_data['mainus'], pollution_data['maincn'],
                weather_data['tp'], weather_data['hu'], weather_data['ws'], weather_data['pr'],
                pollution_data['aqius'], pollution_data['aqicn'], pollution_data['mainus'], pollution_data['maincn'],
            )
        )
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

load_to_postgres = PythonOperator(
    task_id='load_airvisual_to_postgresql',
    python_callable=load_airvisual_to_postgresql,
)

Then, dependencies:

get_airvisual_data >> read_data >> load_to_postgres

But wait, the task IDs in the code are 'get_airvisual_data_hourly', 'read_data_airvisual', 'load_airvisual_to_postgresql'. So the PythonOperator task_ids should match those.

Wait, in the code above, the first task is named 'get_airvisual_data_hourly' as the task_id. So:

get_airvisual_data = PythonOperator(
    task_id='get_airvisual_data_hourly',
    ...
)

read_data = PythonOperator(
    task_id='read_data_airvisual',
    ...
)

load_to_postgres = PythonOperator(
    task_id='load_airvisual_to_postgresql',
    ...
)

Then dependencies:

get_airvisual_data >> read_data >> load_to_postgres

Putting all together.

Potential issues:

- The paths for the JSON files (like '/tmp/airvisual_data.json') might need to be adjusted based on the Airflow environment. But the user mentioned 'local JSON files', so assuming the path is accessible.

- The mapping files 'pollution_mapping.json' and 'weather_mapping.json' are assumed to be in the current working directory. If they're elsewhere, the paths need to be specified correctly.

- The SQL statements for inserting into the tables may need to match the actual schema. The problem description mentions conflict resolution, so using ON CONFLICT clauses.

- The datetime handling: converting the API timestamp to Bangkok time, then checking for existing data based on the hour. The code truncates the datetime to the hour (datetime_value = local_time.replace(minute=0, second=0, microsecond=0)).

- The fact table insertion uses subqueries to get the location and pollution IDs. This assumes that the dim tables have unique constraints on those columns.

- Error handling: using try/except with rollback on error.

- The functions use print statements; better to use logging, but the problem says to make it executable, so print is acceptable for simplicity.

- The PostgresHook usage: in Airflow 2.x, the PostgresHook is from the postgres provider, which needs to be installed. But the user said to assume required packages are installed.

Now, putting all the code together with proper imports, DAG definition, and tasks.
</think>

from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pytz
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

def get_airvisual_data_hourly(**context):
    api_key = Variable.get("AIRVISUAL_API_KEY")
    coords = Variable.get("SALAYA_COORDINATES").split(',')
    lat, lon = coords[0].strip(), coords[1].strip()
    
    url = f"https://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Convert timestamp to Bangkok timezone
    ts = data['data']['current']['pollution']['ts']
    utc_dt = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)
    bkk_dt = utc_dt.astimezone(pytz.timezone('Asia/Bangkok'))
    data['data']['current']['pollution']['ts'] = bkk_dt.isoformat()
    
    # Check for existing records
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM factairvisualtable 
            WHERE datetime = %s::timestamp
        """, (bkk_dt.replace(minute=0, second=0, microsecond=0),))
        if cur.fetchone():
            raise AirflowSkipException('Data exists for current hour')
    
    # Atomic file write
    tmp_path = '/tmp/airvisual_tmp.json'
    final_path = '/tmp/airvisual_data.json'
    with open(tmp_path, 'w') as f:
        json.dump(data, f)
    os.replace(tmp_path, final_path)

def read_data_airvisual(**context):
    if not os.path.exists('/tmp/airvisual_data.json'):
        raise FileNotFoundError('Temporary data file missing')
    
    with open('/tmp/airvisual_data.json', 'r') as f:
        data = json.load(f)
    
    if not all(k in data.get('data', {}) for k in ['city', 'current']):
        raise ValueError('Invalid data structure from API response')
    
    context['ti'].xcom_push(key='api_data', value=data)

def load_airvisual_to_postgresql(**context):
    with open('/tmp/airvisual_data.json', 'r') as f:
        data = context['ti'].xcom_pull(task_ids='read_data_airvisual', key='api_data') or json.load(f)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    
    try:
        with conn.cursor() as cur:
            # Load mapping configs
            with open('pollution_mapping.json') as f:
                pollution_map = json.load(f)
            with open('weather_mapping.json') as f:
                weather_map = json.load(f)
            
            # Insert datetime dimension
            dt = datetime.fromisoformat(data['data']['current']['pollution']['ts'])
            cur.execute("""
                INSERT INTO dimDateTimeTable (datetime, timestamp, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (datetime) DO NOTHING
            """, (dt, dt, dt.year, dt.month, dt.day, dt.hour))
            
            # Insert location dimension
            loc = data['data']['city']
            cur.execute("""
                INSERT INTO dimLocationTable (city, country, state)
                VALUES (%s, %s, %s)
                ON CONFLICT (city, country, state) DO NOTHING
            """, (loc['city'], loc['country'], loc.get('state', '')))
            
            # Insert pollution metadata
            poll = data['data']['current']['pollution']
            cur.execute("""
                INSERT INTO dimMainPollutionTable (aqius, aqicn, mainus, maincn)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (aqius, aqicn, mainus, maincn) DO NOTHING
            """, (poll['aqius'], poll['aqicn'], 
                  pollution_map.get(str(poll['mainus']), 'unknown'),
                  pollution_map.get(str(poll['maincn']), 'unknown')))
            
            # Insert fact data
            weather = data['data']['current']['weather']
            cur.execute("""
                INSERT INTO factairvisualtable (
                    datetime, location_id, pollution_id, 
                    temperature, humidity, windspeed, pressure,
                    aqius, aqicn, mainus, maincn
                )
                SELECT %s, l.id, p.id, %s, %s, %s, %s, %s, %s, %s, %s
                FROM dimLocationTable l, dimMainPollutionTable p
                WHERE l.city = %s AND l.country = %s
                  AND p.aqius = %s AND p.aqicn = %s
                """, (
                    dt, weather['tp'], weather['hu'], weather['ws'], weather['pr'],
                    poll['aqius'], poll['aqicn'], poll['mainus'], poll['maincn'],
                    loc['city'], loc['country'], poll['aqius'], poll['aqicn']
                ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

with DAG(
    dag_id='airvisual_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['air_quality'],
) as dag:

    fetch_data = PythonOperator(
        task_id='get_airvisual_data_hourly',
        python_callable=get_airvisual_data_hourly,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    validate_data = PythonOperator(
        task_id='read_data_airvisual',
        python_callable=read_data_airvisual,
    )

    load_data = PythonOperator(
        task_id='load_airvisual_to_postgresql',
        python_callable=load_airvisual_to_postgresql,
    )

    fetch_data >> validate_data >> load_data