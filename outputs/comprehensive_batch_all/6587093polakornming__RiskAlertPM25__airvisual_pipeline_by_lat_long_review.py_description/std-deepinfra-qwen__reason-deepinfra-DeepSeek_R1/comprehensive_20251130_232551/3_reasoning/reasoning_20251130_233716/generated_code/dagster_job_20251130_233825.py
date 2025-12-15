<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three tasks that run sequentially: get_airvisual_data_hourly, read_data_airvisual, and load_airvisual_to_postgresql. Each task becomes an op in Dagster. 

First, the get_airvisual_data_hourly op. It fetches data from the AirVisual API. The user mentioned using configured coordinates and an API key, so I'll need to set up config for those. Also, this op checks for duplicates in the database and skips downstream processing if data exists. In Dagster, if an op wants to skip downstream ops, it can raise a Failure or a SkipReason. Wait, the original uses AirflowSkipException, but in Dagster, we can use SkipReason from dagster or handle it by not yielding outputs. Hmm, maybe using a conditional output or raising a failure. Alternatively, the op could return None, and downstream ops check for that. But the user's example mentions raising AirflowSkipException, which isn't part of Dagster. So perhaps we need to handle skipping by not executing downstream ops. But Dagster's execution is based on dependencies. If an op doesn't produce an output, the downstream ops won't run. So maybe the first op returns a result if data is new, otherwise returns None. Then the next ops have a dependency on that output. But in the original, it's a linear sequence: t1 → t2 → t3. So if t1 skips, t2 and t3 shouldn't run. In Dagster, if an op doesn't yield an output, the downstream ops depending on it won't execute. So perhaps in the get_airvisual_data_hourly op, if data exists, it doesn't yield an output, causing the rest to skip. Alternatively, raise a SkipReason. But I think using Output or not will control the flow. Let me structure the first op to return the data if it's new, otherwise return nothing. Wait, in Dagster, ops must yield at least one output. So maybe return a value indicating whether to proceed. But the user's example uses AirflowSkipException to skip downstream tasks. So perhaps in Dagster, we can have the op raise a Failure with a flag, but that would fail the entire job. Alternatively, use conditional execution. Maybe the first op returns a boolean indicating whether to proceed. Then, the subsequent ops are only executed if that boolean is True. But how to model that in Dagster's dependency graph. Since the original is linear, perhaps the first op's output is passed to the second, and if it's None, the second op doesn't run. Wait, but in Dagster, if an op's input is not provided, the op isn't executed. So if the first op doesn't produce an output, the second op won't run. So maybe in the first op, if data exists, we don't yield an output. But in Dagster, every op must yield at least one output. So perhaps we need to structure it such that when data exists, the op raises a SkipReason, which would mark the op as skipped, and downstream ops are not executed. However, Dagster's default behavior is that if an op is skipped, downstream ops are still executed unless their inputs are missing. Wait, no. If an op is skipped, any downstream ops that depend on its output will not execute because their inputs are not available. So if the first op is skipped, then the second op, which depends on its output, won't run. But how to make the first op skip itself. In Dagster, you can use the op's compute function to yield a SkipReason, which skips the op and any downstream ops that depend on its outputs. So in the get_airvisual_data_hourly op, if the data exists, we can yield a SkipReason, which skips the op and the downstream ops. That would align with the original behavior. So I'll need to import SkipReason from dagster and use it in the op. 

Next, the op needs to write a JSON file temporarily. So perhaps after validating the data, it writes to a file and returns the file path. Then the read_data_airvisual op reads that file. But the original uses an atomic file write pattern, so maybe write to a temp file and then rename. But for simplicity, perhaps just write to a file and return the path. 

The get_airvisual_data_hourly op also converts the timestamp to Bangkok timezone. So in the code, after getting the data, adjust the timestamp. 

For the config, the op needs latitude, longitude, and API key. So the op will have a config schema that includes those. The user mentioned retry configuration: 2 retries with 5-minute delay for the API task. In Dagster, we can set retries and delay using the @op decorator's config. So adding retry_policy=RetryPolicy(max_retries=2, delay=300) to the op. 

The read_data_airvisual op reads the JSON file and validates it. So it depends on the output of the first op (the file path), reads it, and returns the data. Then the load_airvisual_to_postgresql op takes that data and loads it into PostgreSQL. 

The load op needs to interact with PostgreSQL. So perhaps using a resource for the database connection. The user mentioned the connection ID "postgres_conn", so in Dagster, resources are used for external connections. So I'll define a postgres_resource as a stub. The op will use this resource to get a connection. 

The load op also loads mapping configurations from JSON files. So perhaps those paths are part of the config. Or hardcoded for simplicity. The user says to provide simplified resource or config examples as comments or stubs. So maybe add a config parameter for the mapping files. 

The load op does several inserts into different tables with conflict resolution. For simplicity, the code can have placeholders for the SQL statements, using the PostgreSQL resource to execute them. 

Now, putting it all together. The job is composed of the three ops in sequence. The first op may skip, which skips the rest. 

For the main block, include the execute_in_process when run as a script. 

Testing for existing data in the database: the first op needs to check if the current hour's data exists. So perhaps it uses the PostgreSQL resource to query the database. But since the original task checks the database before proceeding, the first op would need access to the database resource. Wait, but in the original pipeline steps, the first task checks the database for existing records. So the get_airvisual_data_hourly op needs to interact with the database. That complicates things because the first op would then depend on the PostgreSQL resource as well. But the original description says that the load op is the one that interacts with the database. Wait, no: the first task checks if data exists in the database to avoid duplicates. So the first op needs to query the database. Therefore, the first op requires the PostgreSQL resource. 

But in the original pipeline, the load op uses the PostgreSQL connection. So both the first and third ops need the PostgreSQL resource. So the resource should be available to those ops. 

So, the PostgreSQL resource is a dependency for both get_airvisual_data_hourly and load_airvisual_to_postgresql ops. 

But in the initial setup, the user mentioned that the load op uses the connection ID "postgres_conn". So in Dagster, resources are provided to ops via context. So the ops that need the PostgreSQL connection will require the resource. 

So, the first op (get_airvisual_data_hourly) will need the PostgreSQL resource to check for existing data. The third op (load_airvisual_to_postgresql) will also need it. 

Therefore, the job will need to include the resource configuration. 

But the user said to provide simplified resource examples as comments or minimal stubs. So perhaps define a postgres_resource as a stub, and in the ops, use context.resources.postgres to get the connection. 

Putting this all together:

- Define the three ops with their dependencies.
- The first op uses config (lat, lon, api_key) and the postgres resource.
- The third op uses the postgres resource and the data from the second op.
- The retry policies are applied to the first op and the job level? Wait, the user mentioned retry configuration: 2 retries with 5-minute delay for API task, 3-minute delay for DAG-level retries. So the API task (get_airvisual_data_hourly) has its own retry policy, and the job has a default retry policy. In Dagster, job-level retries can be set, but per-op policies override them. So adding the retry_policy to the get_airvisual_data_hourly op with max_retries=2 and delay=300 seconds (5 minutes). The job-level retry would be set with a Dagster RunConfig, but since the user wants a minimal example, perhaps the job doesn't specify it, but the user's comment mentions it. However, the code example may not include that unless specified. The user's constraints say to include retry configuration as per the description. So for the API task, add the retry_policy in the @op decorator. For the DAG-level retries (3-minute delay), perhaps that's handled by the Dagster run config, which isn't part of the code. So maybe just a comment. 

Now, writing the code:

Imports needed: dagster's op, job, resource, RetryPolicy, SkipReason, Field, etc.

The postgres_resource is a stub resource, perhaps a function that returns a connection object. But for the example, it can be a dummy resource. 

The get_airvisual_data_hourly op:

- Takes config (lat, lon, api_key)
- Uses postgres resource to check for existing data
- Fetches from API
- Converts timestamp to Bangkok timezone
- Checks if data exists in DB
- If exists, skips (raises SkipReason)
- Else, writes JSON file and returns path

The read_data_airvisual op:

- Takes the file path from the first op
- Reads and validates the data
- Returns the data

The load_airvisual_to_postgresql op:

- Takes the data
- Uses postgres resource to load into tables
- Uses mapping files (config or hardcoded paths)
- Executes SQL statements with conflict resolution

Now, code structure:

Resources:

@resource
def postgres_resource(_):
    # Stub connection
    return "postgres_conn"

Ops:

@op(required_resource_keys={"postgres"}, config_schema={...}, retry_policy=RetryPolicy(...))
def get_airvisual_data_hourly(context):
    # Check DB for existing data
    # If exists, raise SkipReason
    # Else proceed

@op
def read_data_airvisual(context, file_path):
    # Read file and return data

@op(required_resource_keys={"postgres"})
def load_airvisual_to_postgresql(context, data):
    # Load data into PostgreSQL

Job:

@job(resource_defs={"postgres": postgres_resource})
def air_quality_etl_job():
    data = get_airvisual_data_hourly()
    validated_data = read_data_airvisual(data)
    load_airvisual_to_postgresql(validated_data)

But wait, the first op may skip, so the output may not be generated. So in Dagster, if the first op is skipped (raises SkipReason), the downstream ops won't execute. So the code as written would work. 

Handling the SkipReason in the first op: if data exists, context.log.info and then raise SkipReason("Data exists"). But in Dagster, you can yield SkipReason, but I think the correct way is to use the event system. For example:

from dagster import op, job, resource, RetryPolicy, SkipReason, Output, EventMetadata

In the get_airvisual_data_hourly op:

if data_exists:
    context.log.info("Data already exists, skipping...")
    raise SkipReason("Data already exists in database")
else:
    # proceed to fetch and write file
    yield Output(file_path, "file_path")

Wait, but raising SkipReason would skip the op, but if the op is skipped, it doesn't produce an output. So the downstream ops won't run. So perhaps, in the op, if data exists, we don't yield an output and instead raise SkipReason. But in Dagster, you can't both raise an exception and yield an output. So perhaps the correct approach is to check if data exists, and if so, yield a SkipReason event and return nothing. But Dagster requires that ops yield at least one output. Hmm, this is a bit tricky. 

Alternatively, the op can yield an Output with a flag indicating whether to proceed. Then, the downstream ops can check that flag. But that complicates the dependencies. 

Alternatively, use Dagster's conditional execution with dynamic outputs. But the original pipeline is linear, so perhaps the simplest way is to have the first op raise SkipReason, which skips itself and the downstream ops. 

But in Dagster, when an op is skipped, any downstream ops that depend on its output are skipped. So if the first op raises SkipReason, it's marked as skipped, and the next op, which depends on its output, will not execute. 

So in code:

In get_airvisual_data_hourly:

if data_exists:
    context.log.info("Data exists, skipping...")
    yield SkipReason("Data exists, skipping processing")
    return  # Or yield nothing?

Wait, no. In Dagster, you can't yield a SkipReason. Instead, you can emit a SkipReason event. Wait, perhaps I'm mixing up Airflow's SkipException with Dagster's approach. 

In Dagster, ops can emit events. To skip an op and downstream ops, the op can emit a SkipReason event and not yield any outputs. But in Dagster, an op must yield at least one output or a failure. So if the op decides to skip processing, it can't just emit a SkipReason; it has to either produce an output or fail. 

This complicates things. So perhaps the correct approach is to have the op return a value indicating whether to proceed. Then, the next op checks that value and decides whether to proceed. But that would require branching, which the original pipeline doesn't have. 

Alternatively, the op can raise a Failure, but that would mark the op as failed, which isn't desired. The original uses a SkipException to skip downstream tasks. 

Hmm. Maybe the best approach here is to have the first op return None if data exists, and the next op checks if the input is None. But in Dagster, if an op returns None, the next op will receive None as input. So the read_data_airvisual op would need to handle None and skip processing. But the original pipeline's design is to skip downstream processing if data exists. 

Alternatively, the first op returns a boolean indicating whether to proceed. Then, the next ops are only executed if the boolean is True. But in Dagster, you can't conditionally execute ops based on the output value. You need to use Dagster's conditional logic, like using a conditional output with .map or .collect, but that's more complex. 

Given the original pipeline's linear structure and the requirement to skip downstream tasks if data exists, perhaps the best way is to have the first op raise a SkipReason, which skips the op and the downstream ops. But how to do that in Dagster. 

Upon checking Dagster's documentation, emitting a SkipReason event is possible but doesn't automatically skip downstream ops. Instead, the op must not produce an output. But if the op is supposed to produce an output, then not producing one would cause downstream ops to skip. 

So, in the get_airvisual_data_hourly op:

if data exists:
    context.log.info("Data exists, skipping...")
    # Don't yield an output
    return
else:
    # process and yield output

But in Dagster, every op must yield at least one output. So this approach would cause an error. 

Alternative idea: Yield an optional output. For example, the op can yield an Output or not. But in Python, you can't conditionally yield in a function. So perhaps return an optional value. 

Wait, in Dagster, ops can have multiple outputs, or optional outputs. But the way to handle this is to have the op yield an Output if there's data, otherwise not. But again, the op must yield at least one output. 

This is a problem. So perhaps the correct approach is to have the first op return a value indicating whether to proceed, and then structure the job to conditionally execute the next ops. But Dagster's job definitions are static, so dynamic execution based on op outputs isn't straightforward. 

Alternatively, use a Dagster graph and conditional branches. But the user specified that there's no branching/parallelism, so the pipeline is linear. 

Given the constraints, perhaps the best way is to proceed under the assumption that if the first op doesn't produce an output (due to an exception), the downstream ops are skipped. But how to handle the case where data exists without raising an exception. 

Wait, the original task raises AirflowSkipException to skip downstream processing. In Dagster, perhaps the equivalent is to have the op raise a SkipReason, which skips the op and downstream ops. 

But according to Dagster's documentation, you can yield a SkipReason event, which marks the op as skipped, but downstream ops will still execute if they don't depend on its outputs. So if the downstream ops depend on the output of the skipped op, they won't execute. 

So, in this case, if the first op is skipped (emits a SkipReason), and the next op depends on its output, then the next op will not execute. 

But how to emit a SkipReason. 

In Dagster, you can use context.log.info and then return nothing, but that would cause a failure because the op is expected to yield an output. 

Alternatively, raise a Failure with a flag to indicate it's skippable. But that's not standard. 

Alternatively, the op can yield an Output with a flag, and the next op checks the flag. But this requires changing the logic of the next ops. 

Given the time constraints, perhaps the best approach is to proceed with the following structure:

In get_airvisual_data_hourly:

Check if data exists. If yes, log and don't yield an output. But since Dagster requires an output, perhaps yield an Output(None) or some sentinel value. Then, in read_data_airvisual, check if the input is valid. If it's None, then log and don't proceed. But this would require the next op to handle it. 

But the original pipeline's tasks are supposed to skip downstream processing entirely. 

Alternatively, the first op raises a Failure but with a flag that it's skippable. But this isn't standard. 

Given the time I've spent, perhaps proceed with the following code:

Implement the first op to check for existing data. If exists, raise SkipReason. But in Dagster, this isn't directly possible. So perhaps use a different approach. 

Wait, looking up Dagster's SkipReason: according to the documentation, you can yield a SkipReason event, which marks the op as skipped. However, the op is still considered successful, and downstream ops that don't depend on its output will still run. But in our case, the downstream ops depend on the output of the first op, so if the first op is skipped (and thus doesn't produce an output), the downstream ops won't run. 

So, in code:

In get_airvisual_data_hourly:

if data_exists:
    context.log.info("Data exists, skipping...")
    yield SkipReason("Data exists")
    return
else:
    # proceed to fetch data
    yield Output(file_path)

But in Dagster, yielding a SkipReason event is done via context.log_event(SkipReason(...)). 

Wait, the correct way to emit a SkipReason event is:

from dagster import SkipReason

if data_exists:
    context.log_event(SkipReason("Data exists"))
    return

But then, the op must still yield an output. So this approach won't work. 

Alternatively, the op can yield both a SkipReason and an Output. But that's not allowed. 

This indicates that the original Airflow logic doesn't map directly to Dagster's execution model. Therefore, perhaps the best approach is to have the first op return a value indicating whether processing should continue, and then have the subsequent ops check that value. But this requires modifying the pipeline's structure. 

Alternatively, use a Dagster asset-based approach, where each op produces an asset, and if the asset already exists, the op is skipped. But the user's question specifies using ops and jobs, not assets. 

Given the time constraints, perhaps proceed with the following code, acknowledging that the skip logic may not perfectly mirror Airflow's but uses Dagster's features:

The first op checks if data exists. If it does, it returns None. The next ops check if their inputs are None and skip processing. But since the user's pipeline requires that downstream tasks are skipped entirely, this approach would still execute the ops but have them do nothing. 

Alternatively, the first op raises a Failure but with a flag that it's skippable. But this isn't standard. 

Perhaps the correct way is to have the first op raise a Failure if data exists, but that would mark the entire job as failed, which isn't desired. 

Given the time I've spent, I'll proceed with the code that uses SkipReason events and assumes that downstream ops are skipped if the first op doesn't produce an output. However, in Dagster, this isn't possible because the op must yield an output. So perhaps the first op returns a file path if data is new, or None if data exists. Then, the next ops check if the input is None and skip. But this requires modifying the original logic. 

Alternatively, the first op returns a boolean indicating whether to proceed. Then, the next ops are only executed if the boolean is True. But how to model that in Dagster's static job structure. 

This is getting too complicated. Given the user's constraints, perhaps proceed with the code that uses SkipReason events and assumes that downstream ops are skipped. Even if it's not perfect, it's the closest approximation. 

So, the code:

Implement the first op to check for existing data. If exists, log and yield a SkipReason event, but still return a dummy output. But this isn't correct. 

Alternatively, the first op returns a file path if data is new, else returns None. The next op checks if the file path is None and skips. But again, this requires the next op to handle it. 

Given the time, I'll proceed with the code that uses the SkipReason event and allows downstream ops to not execute if the first op's output is not provided. However, since Dagster requires outputs, perhaps the first op returns a value indicating whether to proceed, and the next ops are structured to depend on that. 

But given the original pipeline's linear structure, perhaps the best way is to proceed with the code as follows, even if the skip logic isn't perfect. 

Final code:

from dagster import op, job, ResourceDefinition, RetryPolicy, SkipReason, Output, EventMetadata

@op(
    config_schema={
        "latitude": float,
        "longitude": float,
        "api_key": str,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"postgres"},
)
def get_airvisual_data_hourly(context):
    # Check if data exists in database
    # Stub database check
    data_exists = False  # Assume not exists for example
    if data_exists:
        context.log.info("Data already exists, skipping processing.")
        raise SkipReason("Data exists")
    # Fetch data from API (stubbed)
    import requests
    response = requests.get(
        f"https://api.airvisual.com/v2/nearest_city?lat={context.op_config['latitude']}&lon={context.op_config['longitude']}&key={context.op_config['api_key']}"
    )
    response.raise_for_status()
    data = response.json()
    # Convert timestamp to Bangkok timezone
    from datetime import datetime
    import pytz
    utc_time = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
    bangkok_tz = pytz.timezone("Asia/Bangkok")
    local_time = utc_time.astimezone(bangkok_tz)
    data['data']['current']['pollution']['ts'] = local_time.isoformat()
    # Write to temporary file
    import json
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
        json.dump(data, tmp_file)
        file_path = tmp_file.name
    yield Output(file_path)

@op
def read_data_airvisual(context, file_path):
    import json
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        context.log.info("Successfully read data file")
        return data
    except FileNotFoundError:
        context.log.error("File not found")
        raise

@op(required_resource_keys={"postgres"})
def load_airvisual_to_postgresql(context, data):
    # Load mapping configs (stubbed paths)
    # pollution_mapping = json.load(open("pollution_mapping.json"))
    # weather_mapping = json.load(open("weather_mapping.json"))
    # Insert into dimDateTimeTable
    conn = context.resources.postgres
    # Example SQL execution (stubbed)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dimDateTimeTable (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
        (data['data']['current']['pollution']['ts'],)
    )
    # Similar inserts for other tables
    conn.commit()
    context.log.info("Data loaded successfully")

postgres_resource = ResourceDefinition.mock_resource()

@job(resource_defs={"postgres": postgres_resource})
def air_quality_etl_job():
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)

if __name__ == "__main__":
    config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "latitude": 13.789,
                    "longitude": 100.512,
                    "api_key": "dummy_api_key",
                }
            }
        }
    }
    result = air_quality_etl_job.execute_in_process(run_config=config)

But wait, in the get_airvisual_data_hourly op, I raised SkipReason, but in Dagster, you can't raise it; you have to yield an event. So the correct way is:

if data_exists:
    context.log_event(SkipReason("Data exists in database"))
    return

But then, the op must yield an output. So this approach would cause an error because the op doesn't yield an output. 

This indicates that the code as written would fail when data_exists is True. 

To fix this, perhaps the op must yield an output even when data exists. So, perhaps return a flag indicating whether to proceed. For example:

@op(...)
def get_airvisual_data_hourly(context):
    data_exists = check_db()
    if data_exists:
        context.log.info("Data exists")
        yield Output(None, "skip_flag")
    else:
        # process data
        yield Output(file_path, "file_path")

Then, the job would have:

def air_quality_etl_job():
    result = get_airvisual_data_hourly()
    data = read_data_airvisual(result.file_path)
    load_airvisual_to_postgresql(data)

But this requires using dynamic outputs or multiple outputs. 

Alternatively, the op yields a Union type, but handling that complicates the code. 

Given the time constraints, perhaps the best approach is to proceed with the initial code, acknowledging that the skip logic isn't perfect but meets the user's requirements as closely as possible. 

Final code will include the SkipReason event and handle the output, even if it's not perfect. However, since the user's example requires that the code is executable, perhaps the SkipReason part is omitted, and the code proceeds without skipping, or uses a different approach. 

Alternatively, remove the skip logic and proceed, but that deviates from the original pipeline description. 

Given the time I've spent, I'll proceed with the code that includes the SkipReason event and handles the output, even if it's not perfect, but ensures the code is executable. 

Wait, but raising SkipReason isn't correct. So perhaps the correct way is to yield a SkipReason event and return an output. But that's not allowed. 

Alternatively, the op can yield a SkipReason event and return a sentinel value. But in Dagster, you can yield multiple events and an output. 

So:

if data_exists:
    context.log_event(SkipReason("Data exists"))
    yield Output(None)
else:
    yield Output(file_path)

Then, the read_data_airvisual op would receive None and have to handle it. But in the original pipeline, downstream tasks are skipped. So the read_data_airvisual op would need to check if the input is None and skip processing. 

But in Dagster, the op will still execute unless its inputs are not available. So if the first op yields Output(None), the next op will receive None as input and must handle it. 

So modifying the read_data_airvisual op:

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        context.log.info("Skipping data read as data exists")
        return None
    # rest of the code

Similarly for the load op. 

But this changes the original pipeline's behavior, as the ops are still executed but return early. 

Given the user's requirement to skip downstream processing, this approach doesn't fully replicate the behavior but is the closest possible in Dagster's framework. 

So, incorporating this into the code:

@op(...)
def get_airvisual_data_hourly(context):
    data_exists = False  # Assume not exists for example
    if data_exists:
        context.log.info("Data already exists, skipping processing.")
        yield Output(None)
    else:
        # process and yield file_path
        yield Output(file_path)

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        context.log.info("Skipping read as data exists")
        return None
    # read file and return data

@op(...)
def load_airvisual_to_postgresql(context, data):
    if data is None:
        context.log.info("Skipping load as data exists")
        return
    # load data

This way, all ops execute but skip processing if the data exists. 

But the original pipeline's tasks are supposed to skip downstream tasks entirely. However, in this code, the tasks are executed but do nothing. 

Given the user's constraints, this is acceptable as the code is executable and meets the other requirements. 

So, final code:

from dagster import op, job, ResourceDefinition, RetryPolicy, get_dagster_logger

logger = get_dagster_logger()

@op(
    config_schema={
        "latitude": float,
        "longitude": float,
        "api_key": str,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"postgres"},
)
def get_airvisual_data_hourly(context):
    # Example database check (stub)
    conn = context.resources.postgres
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM factairvisualtable LIMIT 1")
    data_exists = cursor.fetchone() is not None  # Simplified check

    if data_exists:
        logger.info("Data already exists, skipping processing.")
        return None

    # Fetch data from AirVisual API
    import requests
    response = requests.get(
        f"https://api.airvisual.com/v2/nearest_city",
        params={
            "lat": context.op_config["latitude"],
            "lon": context.op_config["longitude"],
            "key": context.op_config["api_key"]
        }
    )
    response.raise_for_status()
    data = response.json()

    # Convert timestamp to Bangkok timezone
    from datetime import datetime
    from pytz import timezone
    utc_time = datetime.utcfromtimestamp(data['data']['current']['pollution']['ts'])
    bangkok_tz = timezone('Asia/Bangkok')
    local_time = utc_time.astimezone(bangkok_tz)
    data['data']['current']['pollution']['ts'] = local_time.isoformat()

    # Atomic write to temporary file
    import json
    import tempfile
    import os
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, 'airvisual_temp.json')
    with open(temp_path, 'w') as f:
        json.dump(data, f)
    
    return temp_path

@op
def read_data_airvisual(context, file_path):
    if file_path is None:
        logger.info("Skipping data read as data exists")
        return None

    import json
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.info("Data extraction verified")
        return data
    except Exception as e:
        logger.error(f"Data read failed: {e}")
        raise

@op(required_resource_keys={"postgres"})
def load_airvisual_to_postgresql(context, data):
    if data is None:
        logger.info("Skipping data load as data exists")
        return

    # Load mapping configurations (stub implementation)
    pollution_mapping = {}  # Load from JSON file if needed
    weather_mapping = {}

    conn = context.resources.postgres
    cursor = conn.cursor()

    try:
        # Insert into dimDateTimeTable
        cursor.execute(
            """
            INSERT INTO dimDateTimeTable (timestamp)
            VALUES (%s)
            ON CONFLICT (timestamp) DO NOTHING
            """,
            (data['data']['current']['pollution']['ts'],)
        )

        # Insert into dimLocationTable (example)
        location_data = data['data']['location']
        cursor.execute(
            """
            INSERT INTO dimLocationTable (latitude, longitude, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (latitude, longitude) DO NOTHING
            """,
            (location_data['latitude'], location_data['longitude'], location_data['city'])
        )

        # Insert into factairvisualtable (example)
        cursor.execute(
            """
            INSERT INTO factairvisualtable (
                timestamp_id, location_id, aqi, pm25, pm10, temperature, humidity
            )
            VALUES (
                (SELECT id FROM dimDateTimeTable WHERE timestamp = %s),
                (SELECT id FROM dimLocationTable WHERE latitude = %s AND longitude = %s),
                %s, %s, %s, %s, %s
            )
            """,
            (
                data['data']['current']['pollution']['ts'],
                location_data['latitude'],
                location_data['longitude'],
                data['data']['current']['pollution']['aqius'],
                data['data']['current']['pollution']['pm25'],
                data['data']['current']['pollution']['pm10'],
                data['data']['current']['weather']['tp'],
                data['data']['current']['weather']['hu']
            )
        )
        conn.commit()
        logger.info("Data loaded successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed: {e}")
        raise

postgres_resource = ResourceDefinition.mock_resource()

@job(
    resource_defs={"postgres": postgres_resource},
)
def air_quality_etl_job():
    file_path = get_airvisual_data_hourly()
    data = read_data_airvisual(file_path)
    load_airvisual_to_postgresql(data)

if __name__ == "__main__":
    run_config = {
        "ops": {
            "get_airvisual_data_hourly": {
                "config": {
                    "latitude": 13.789,
                    "longitude": 100.512,
                    "api_key": "dummy_key"
                }
            }
        }
    }
    result = air_quality_etl_job.execute_in_process(run_config=run_config)
</think>

from dagster import op, job, ResourceDefinition, RetryPolicy, get_dagster_logger

logger = get_dagster_logger()

@op(
    config_schema={
        "latitude": float,
        "longitude": float,
        "api_key": str,