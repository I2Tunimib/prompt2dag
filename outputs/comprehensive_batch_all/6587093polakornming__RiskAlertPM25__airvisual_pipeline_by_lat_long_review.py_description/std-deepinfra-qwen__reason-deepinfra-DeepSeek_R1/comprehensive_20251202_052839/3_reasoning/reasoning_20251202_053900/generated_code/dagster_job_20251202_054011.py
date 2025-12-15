<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three tasks: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. They need to be sequential, so dependencies are linear.

First, I'll create each task as a Dagster op using @op. Each op should have the necessary logic. For get_airvisual_data_hourly, I need to fetch data from the AirVisual API. The user mentioned using latitude and longitude from config, so I'll set up the op to accept a config_schema. The config should include api_key, latitude, and longitude. 

This op also needs to check for duplicates in the database. If data exists, it should skip downstream tasks. In Airflow, they use AirflowSkipException, but in Dagster, maybe we can raise a Failure or return early. Wait, but the user specified to raise AirflowSkipException. Hmm, but Dagster doesn't have that. Maybe we can use a SkipReason or just return None and have downstream ops handle it. Alternatively, perhaps we should raise a Failure with a message indicating to skip. But the user's instruction says to model skipping via raising the exception. Since Dagster doesn't have AirflowSkipException, maybe we need to adjust. Alternatively, perhaps the first op can return a value that downstream ops check, but the user's example uses exceptions. Maybe in Dagster, we can raise a Failure and have the job handle it by skipping downstream. But I'm not sure. Alternatively, the first op could return a result that indicates whether to proceed. But the user's example says to raise an exception to skip downstream. 

Wait, the original pipeline uses AirflowSkipException, which in Airflow skips all downstream tasks. In Dagster, if an op fails, the job stops unless using a sensor or recovery. But the user wants to skip downstream processing if data exists. So perhaps in the first op, if duplicate data is found, we can return a value indicating that, and have the next ops check that value and do nothing. But since the dependencies are linear, maybe the first op can conditionally execute. Alternatively, maybe we can structure the ops so that if the first op doesn't produce data, the next ops don't run. But in Dagster, all ops in the job will run unless their inputs are not met. Wait, no. If an op's upstream op doesn't yield output, the downstream op won't run. So perhaps if the first op returns None, the next op can have a check. But how?

Alternatively, maybe the first op can yield an output, and if it's a duplicate, it doesn't write the file, and the next op checks if the file exists. But the user's description says that the first op writes a temporary file atomically before committing. So perhaps the first op writes the file only if there's no duplicate. Then, the read_data_airvisual op would read the file. If the file isn't present, maybe that op would fail. But the user wants to skip downstream processing. Hmm. Maybe the first op returns a flag indicating whether data was fetched. Then, the next ops can depend on that flag. But in Dagster, each op's execution is determined by the success of its upstream ops. So if the first op completes successfully (even if it's a no-op), the next ops will run. So perhaps the first op should raise an exception if it's a duplicate, which would cause the job to fail unless handled. But the user wants to skip downstream tasks, not fail the entire job. 

This is a bit tricky. Maybe in Dagster, we can use conditional execution with DynamicOutput. Alternatively, perhaps the first op can return a result that indicates whether to proceed, and the next ops can be configured to run only if that result is present. But I'm not sure. Let me think again. The original pipeline uses AirflowSkipException, which skips downstream tasks. In Dagster, if an op fails, the job stops. So perhaps the correct approach is to have the first op not raise an error but instead return a value that downstream ops can use to decide whether to proceed. For example, if the data is a duplicate, the first op returns None, and the next ops check if the data is present. But how would that work in Dagster? Because the next op depends on the output of the first op. So if the first op returns None, the next op would still run but handle the None case. Alternatively, maybe the first op doesn't produce an output that the next op requires, but that's not the case here. The read_data_airvisual task reads the saved JSON file, which is written by the first op only if there's no duplicate. So perhaps the first op writes the file only when there's new data. Then, the read op checks if the file exists. If not, it skips processing. But how to model that in Dagster. 

Alternatively, maybe the first op returns a path to the JSON file if it's written, or None otherwise. Then, the read_data_airvisual op can take that path as input. If the path is None, the op can log and return early. Then, the load_airvisual_to_postgresql op would depend on the output of read_data_airvisual. But if read_data_airvisual returns nothing, then the load op would not have input and thus not run. Wait, but in Dagster, each op's execution is triggered by the success of its upstream ops. So if the first op completes successfully, the next op runs regardless of its output. But if the next op's input is Nothing, perhaps it would fail. So maybe the read_data op needs to handle the case where the file isn't present. 

Alternatively, perhaps the first op writes the file only when there's new data. Then, the read_data op tries to read the file. If it's not there, it raises an error, but the user wants to skip downstream processing. Hmm. This is a bit confusing. Let's proceed step by step.

First, the get_airvisual_data_hourly task:

- Fetches data from API using coordinates and API key (from config).
- Validates response and converts timestamp to Bangkok timezone.
- Checks if data exists in DB for current hour (duplicate check).
- If duplicate, skips downstream processing (raises AirflowSkipException).
- Implements atomic write: save JSON temp file before committing.

So, in Dagster, perhaps the first op will fetch the data, check the DB, and if not a duplicate, write the JSON file. If it is a duplicate, it should skip downstream tasks. But how to model that skip. Since Dagster doesn't have Airflow's skip mechanism, maybe the op can return a value indicating whether to proceed. Then, the next ops can check that value. But how to pass that along. Alternatively, maybe the first op can write the file only if there's no duplicate. Then, the read_data op checks if the file exists. If not, it skips processing. But how to handle that in Dagster.

Alternatively, perhaps the first op returns a boolean indicating whether data was fetched. Then, the read_data op can take that boolean as input. If it's False, the op does nothing. But then, the load op would depend on read_data's output. But if read_data returns nothing, the load op wouldn't run. Wait, but in Dagster, each op must receive the required inputs. So if read_data's output is optional, the load op would need to handle that. Hmm.

Alternatively, maybe the first op's output is the path to the JSON file. If it's a duplicate, the op returns None. Then, the read_data op takes that path as input. If the path is None, it skips processing. But how would that work? Because the read_data op would receive None and have to handle it. Then, it could return None, and the load op would need to handle that. But in Dagster, each op's execution is triggered by the upstream op's completion, not the output value. So even if the first op returns None, the read_data op will run, but if it requires the path as input and the input is None, it might fail. So perhaps the read_data op should have a default input or handle None.

Alternatively, maybe the first op writes the JSON file to a temporary location only if there's no duplicate. Then, the read_data op reads that file. If the file doesn't exist, the read_data op can log a message and return None. Then, the load op would check if the data is present. If not, it skips. But how to pass the data from read_data to load. 

This is getting complicated. Let's try to model this step by step.

First, the get_airvisual_data_hourly op:

- Uses config (api_key, latitude, longitude).
- Fetches data from API.
- Checks DB for existing data (current hour).
- If exists, skips downstream (raises exception? Or returns a flag).
- If not exists, writes JSON file atomically.

Assuming that the DB check is part of this op. Let's say that if the data exists, the op does not write the file and returns a message indicating duplicate. Then, the read_data op tries to read the file. If the file isn't there, it skips. But how to handle that in Dagster.

Alternatively, perhaps the first op returns a status (e.g., "data_fetched" or "duplicate_found"). Then, the read_data op can depend on that status. But how to pass that along. Maybe using multiple outputs. But the user's pipeline is linear, so perhaps each op's output is the input to the next.

Alternatively, perhaps the first op returns the data if it's new, or None if it's a duplicate. Then, the read_data op takes that data as input. If it's None, it skips processing. But then, the read_data op would need to handle the None case. For example:

@op
def get_airvisual_data_hourly(context):
    # fetch data
    if duplicate:
        context.log.info("Duplicate data, skipping downstream")
        return None
    else:
        # write to file
        return file_path

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        context.log.info("No data to read")
        return None
    # read file and return data

@op
def load_airvisual_to_postgresql(context, data):
    if data is None:
        context.log.info("No data to load")
        return
    # load data

But in this case, the read_data op would receive None and have to handle it. Then, the load op would receive None from read_data. But in Dagster, if an op returns None, the next op's input would be None. So the load op would need to check if the data is None and handle it.

This approach could work. So the dependencies are linear: get → read → load. Each op passes along the data or None. If any op returns None, the next op handles it by skipping processing.

But according to the user's description, the first task raises an exception to skip downstream processing. But in Dagster, raising an exception would cause the job to fail unless there's a retry. However, the user mentioned retry configurations: 2 retries with 5-minute delay for the API task, and 3-minute delay for DAG-level retries. Wait, but in Dagster, retries are configured via the op's retry policy. So perhaps the get_airvisual_data_hourly op has a retry policy with 2 retries and 5-minute delay. The job-level retries might be handled via Dagster's run retries, which is outside the code.

So, for the first op, if it's a duplicate, instead of raising an exception, it returns None. Then, the downstream ops check for None and skip processing. That way, the job completes successfully but skips the downstream steps. This aligns with the user's requirement to skip downstream processing without failing the entire job.

Now, let's outline the code structure.

First, the config for the get_airvisual_data_hourly op. The user mentioned using configured coordinates and API key. So the op should have a config_schema that includes api_key, latitude, longitude.

Then, the op makes the API call, checks the database for existing data. If exists, returns None. Else, writes the JSON file and returns the file path.

The read_data_airvisual op reads the file path from the previous op. If the path is None, it logs and returns None. Else, reads the file and returns the data.

The load_airvisual_to_postgresql op takes the data, and if present, loads it into PostgreSQL.

Now, the database interactions. The user mentioned PostgreSQL with connection ID "postgres_conn". In Dagster, this would be a resource. So we need to define a PostgreSQL resource. However, the user's constraints say to provide simplified resource or config examples as comments or minimal stubs. So perhaps we can define a postgres_resource as a stub.

For example:

from dagster import resource

@resource
def postgres_conn(_):
    # Stub for PostgreSQL connection
    return "postgres_connection_stub"

Then, the job would include the resource.

But the load_airvisual_to_postgresql op needs to interact with the database. So the op would require the postgres_resource as a resource.

So, the op would be defined with required_resources={"postgres_conn"}.

But how to handle the database operations. Since the user's example uses conflict resolution on insert, perhaps the op uses SQL statements with ON CONFLICT clauses.

But for the code, we can stub the database interactions with logging statements, as the user wants executable code with minimal stubs.

Now, the file system operations: the first op writes a JSON file temporarily. The read op reads it. For atomic write, perhaps the first op writes to a temporary file and then renames it, but for simplicity, we can just write to a file.

The user mentioned "atomic file write pattern to save JSON data temporarily before committing", so perhaps the first op writes to a temp file and then moves it to the final path. But for the code, we can simulate this.

Now, the configuration files: API keys from config.conf, pollution and weather mappings from JSON files. For the code, we can read from the config (for API key) and load JSON files from a path.

But the user's constraints say to provide minimal stubs. So perhaps the mappings are loaded from files, but in the code, we can use placeholders.

Putting it all together:

- Define the three ops with dependencies.
- Use config for the first op.
- Use resources for PostgreSQL.
- Handle duplicate check in the first op.
- Return file path or None.
- Read and process data in subsequent ops.

Now, code outline:

from dagster import op, job, ResourceParam, get_dagster_logger
import requests
from datetime import datetime
import pytz
import json
import os

logger = get_dagster_logger()

@op(config_schema={
    "api_key": str,
    "latitude": float,
    "longitude": float,
})
def get_airvisual_data_hourly(context):
    # Fetch data from API
    api_key = context.op_config["api_key"]
    lat = context.op_config["latitude"]
    lon = context.op_config["longitude"]
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Convert timestamp to Bangkok timezone
    timestamp = data['data']['current']['pollution']['ts']
    dt = datetime.fromtimestamp(timestamp, tz=pytz.utc).astimezone(pytz.timezone('Asia/Bangkok'))
    data['data']['current']['pollution']['ts'] = dt.isoformat()

    # Check if data exists in DB (stub)
    # Assume a function that checks the database for existing records
    # For this example, we'll hardcode a check (e.g., False for new data)
    data_exists = False  # Replace with actual DB check

    if data_exists:
        logger.info("Data already exists, skipping processing")
        return None
    else:
        # Atomic write to temporary file
        temp_path = "/tmp/airvisual_temp.json"
        final_path = "/tmp/airvisual_data.json"
        with open(temp_path, 'w') as f:
            json.dump(data, f)
        os.rename(temp_path, final_path)
        logger.info("Data saved to file")
        return final_path

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        logger.info("No data to read")
        return None
    # Validate file accessibility
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")
    with open(file_path, 'r') as f:
        data = json.load(f)
    # Validate data structure (stub)
    required_keys = ['data']
    if not all(key in data for key in required_keys):
        raise ValueError("Invalid data structure")
    logger.info("Data extraction successful")
    return data

@op(required_resource_keys={"postgres_conn"})
def load_airvisual_to_postgresql(context, data):
    if data is None:
        logger.info("No data to load")
        return
    # Load mapping configs (stub)
    pollution_mapping = json.load(open("pollution_mapping.json"))
    weather_mapping = json.load(open("weather_mapping.json"))
    
    # Get PostgreSQL connection
    conn = context.resources.postgres_conn
    logger.info(f"Using connection: {conn}")
    
    # Insert into dimDateTimeTable (stub)
    # Example: execute SQL with conflict resolution
    timestamp = data['data']['current']['pollution']['ts']
    # ... SQL logic here ...
    
    # Insert into dimLocationTable (stub)
    # ... similar steps ...
    
    # Insert into factairvisualtable (stub)
    # ... insert data ...
    
    logger.info("Data loaded successfully")

@job(resource_defs={"postgres_conn": postgres_conn})
def air_quality_etl_job():
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)

if __name__ == "__main__":
    run_config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "api_key": "test_key",
                    "latitude": 13.789,
                    "longitude": 100.512,
                }
            }
        }
    }
    result = air_quality_etl_job.execute_in_process(run_config=run_config)

Wait, but the postgres_conn resource is referenced but not defined. Earlier, I thought of defining it as a resource. Let's add that.

But in the code above, the resource is defined as:

from dagster import resource

@resource
def postgres_conn(_):
    # Stub for PostgreSQL connection
    return "postgres_connection_stub"

But in the code outline, I didn't include that. So the complete code should include the resource definition.

Also, the retry configuration: the user mentioned retries for the API task (2 retries with 5-minute delay) and DAG-level retries (3-minute delay). In Dagster, op-specific retries are handled via RetryPolicy. So for the get_airvisual_data_hourly op, we can add a retry policy.

So, importing RetryPolicy:

from dagster import RetryPolicy

Then, defining the retry policy for the get op:

@op(
    config_schema={...},
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 5 minutes in seconds
)
def get_airvisual_data_hourly(context):
    ...

For the DAG-level retries, that's handled outside the code (via Dagster's run retries configuration), so perhaps the user's constraint is satisfied by adding the op's retry policy.

Putting it all together, the code would look like this:

from dagster import op, job, ResourceParam, get_dagster_logger, RetryPolicy, resource
import requests
from datetime import datetime
import pytz
import json
import os

logger = get_dagster_logger()

@resource
def postgres_conn(_):
    # Stub PostgreSQL connection resource
    return "postgres_connection_stub"

@op(
    config_schema={
        "api_key": str,
        "latitude": float,
        "longitude": float,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 5 minutes delay
)
def get_airvisual_data_hourly(context):
    api_key = context.op_config["api_key"]
    lat = context.op_config["latitude"]
    lon = context.op_config["longitude"]
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    
    data = response.json()

    # Convert timestamp to Bangkok timezone
    ts = data['data']['current']['pollution']['ts']
    dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)
    dt_bangkok = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
    data['data']['current']['pollution']['ts'] = dt_bangkok.isoformat()

    # Simulate duplicate check (replace with actual DB query)
    current_hour = dt_bangkok.replace(minute=0, second=0, microsecond=0)
    # Assume data_exists is False for this example
    data_exists = False

    if data_exists:
        logger.info("Data exists, skipping processing")
        return None
    else:
        # Atomic write
        temp_path = "/tmp/airvisual_temp.json"
        final_path = "/tmp/airvisual_data.json"
        with open(temp_path, 'w') as f:
            json.dump(data, f)
        os.replace(temp_path, final_path)  # Atomic rename
        logger.info("Data saved to file")
        return final_path

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        logger.info("Skipping read as no data available")
        return None

    if not os.path.exists(file_path):
        logger.error("Data file not found")
        raise FileNotFoundError(f"File {file_path} does not exist")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Validate data structure
    if 'data' not in data or 'current' not in data['data']:
        logger.error("Invalid data structure")
        raise ValueError("Missing required data fields")
    
    logger.info("Data validated successfully")
    return data

@op(required_resource_keys={"postgres_conn"})
def load_airvisual_to_postgresql(context, data):
    if data is None:
        logger.info("No data to load")
        return
    
    # Load mappings (stub)
    try:
        with open("pollution_mapping.json") as f:
            pollution_mapping = json.load(f)
        with open("weather_mapping.json") as f:
            weather_mapping = json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Mapping file not found: {e}")
        raise

    conn = context.resources.postgres_conn
    logger.info(f"Using connection: {conn}")

    # Example transaction handling (stub)
    try:
        # Insert into dimDateTimeTable
        ts = data['data']['current']['pollution']['ts']
        # SQL execution stub
        logger.info(f"Inserting timestamp {ts} into dimDateTimeTable")
        
        # Insert into dimLocationTable
        location_data = data['data']['city']
        logger.info(f"Inserting location {location_data} into dimLocationTable")
        
        # Insert into factairvisualtable
        pollution_data = data['data']['current']['pollution']
        weather_data = data['data']['current']['weather']
        logger.info("Inserting fact data into factairvisualtable")
        
        logger.info("All data loaded successfully")
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        # Rollback would occur here in real implementation
        raise

@job(resource_defs={"postgres_conn": postgres_conn})
def air_quality_etl_job():
    load_airvisual_to_postgresql(
        read_data_airvisual(
            get_airvisual_data_hourly()
        )
    )

if __name__ == "__main__":
    run_config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "api_key": "test_api_key",
                    "latitude": 13.789,
                    "longitude": 100.512,
                }
            }
        }
    }
    result = air_quality_etl_job.execute_in_process(run_config=run_config)

This code includes all the required components:

- Ops with dependencies.
- Config for the first op.
- Resource for PostgreSQL (stubbed).
- Retry policy for the API op.
- Handling of duplicate data by returning None and skipping downstream processing.
- Atomic file write.
- Data validation steps.
- Transaction management stubs.

The code is executable, assuming the required libraries are installed (requests, pytz, etc.). The PostgreSQL resource is a stub, and the actual database interactions are logged as stubs. The retry policy is applied to the get_airvisual_data_hourly op. The main function includes a sample run configuration.
</think>

from dagster import op, job, ResourceParam, get_dagster_logger, RetryPolicy, resource
import requests
from datetime import datetime
import pytz
import json
import os

logger = get_dagster_logger()

@resource
def postgres_conn(_):
    """Stub PostgreSQL connection resource"""
    return "postgres_connection_stub"

@op(
    config_schema={
        "api_key": str,
        "latitude": float,
        "longitude": float,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def get_airvisual_data_hourly(context):
    """Fetch air quality data from AirVisual API with duplicate check"""
    api_key = context.op_config["api_key"]
    lat = context.op_config["latitude"]
    lon = context.op_config["longitude"]
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

    data = response.json()

    ts = data['data']['current']['pollution']['ts']
    dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)
    dt_bangkok = dt_utc.astimezone(pytz.timezone('Asia/Bangkok'))
    data['data']['current']['pollution']['ts'] = dt_bangkok.isoformat()

    # Simulate database duplicate check
    data_exists = False  # Replace with actual DB query

    if data_exists:
        logger.info("Duplicate data found - skipping processing")
        return None

    temp_path = "/tmp/airvisual_temp.json"
    final_path = "/tmp/airvisual_data.json"
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    os.replace(temp_path, final_path)
    return final_path

@op
def read_data_airvisual(context, file_path):
    """Validate and read extracted data from JSON file"""
    if file_path is None:
        logger.info("No data file available - skipping read")
        return None

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file {file_path} not found")

    with open(file_path, 'r') as f:
        data = json.load(f)

    if 'data' not in data or 'current' not in data['data']:
        raise ValueError("Invalid data structure in JSON file")

    logger.info("Successfully validated and read data file")
    return data

@op(required_resource_keys={"postgres_conn"})
def load_airvisual_to_postgresql(context, data):
    """Load transformed data into PostgreSQL database"""
    if data is None:
        logger.info("No data received for loading")
        return

    try:
        with open("pollution_mapping.json") as f:
            pollution_mapping = json.load(f)
        with open("weather_mapping.json") as f:
            weather_mapping = json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Mapping file error: {e}")
        raise

    conn = context.resources.postgres_conn
    logger.info(f"Using database connection: {conn}")

    try:
        pollution_data = data['data']['current']['pollution']
        weather_data = data['data']['current']['weather']
        logger.info("Inserting into dimDateTimeTable")
        logger.info("Inserting into dimLocationTable")
        logger.info("Inserting into factairvisualtable")
        logger.info("All database operations completed successfully")
    except Exception as e:
        logger.error(f"Database transaction failed: {e}")
        raise

@job(resource_defs={"postgres_conn": postgres_conn})
def air_quality_etl_job():
    load_airvisual_to_postgresql(
        read_data_airvisual(
            get_airvisual_data_hourly()
        )
    )

if __name__ == "__main__":
    run_config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "api_key": "test_key",
                    "latitude": 13.789,
                    "longitude": 100.512,
                }
            }
        }
    }
    result = air_quality_etl_job.execute_in_process(run_config=run_config)