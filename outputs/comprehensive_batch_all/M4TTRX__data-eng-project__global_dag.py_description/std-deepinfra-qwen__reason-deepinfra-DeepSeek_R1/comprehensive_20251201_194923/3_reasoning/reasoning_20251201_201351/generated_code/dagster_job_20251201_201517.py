<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let me start by understanding the requirements and constraints.

First, the pipeline has multiple steps organized into ingestion, staging, and loading stages. The user wants each step represented as Dagster ops (@op) within a job (@job). The dependencies need to be explicitly set, including parallel execution where mentioned.

Looking at the pipeline steps:

- The process starts, then there's parallel ingestion from four sources. So I'll need four ops for fetching data from each source. These can run in parallel, so their dependencies in the job should be such that they all start after the initial step.

Wait, the first step is "Start pipeline execution". Maybe that's just a placeholder. In Dagster, the job starts automatically, so perhaps that step isn't needed as an op. Maybe the initial ops are the four data fetchers. Or maybe there's a start op that triggers the others. But the description might be more of a logical step. Let me check the dependencies.

The four data ingestion steps are parallel. So those four ops (fetch_nuclear_metadata, fetch_thermal_metadata, fetch_death_resources, fetch_city_geo) should have no dependencies among themselves, meaning they can run in parallel.

Next, processing fetched metadata involves downloading CSV/data files. These steps depend on the previous metadata fetchers. For example, download_nuclear_data would depend on fetch_nuclear_metadata, and similarly for thermal and death records. The city geo might not need further processing here.

Then creating PostgreSQL tables: create_deaths_table and create_power_plants_table. These need to happen before data loading. But when exactly? They should probably come after the metadata fetching and downloading steps, since the data is needed to create the tables. Wait, no—the tables need to be created before loading data into them. So the create tables ops should come after the metadata steps but before the data processing steps.

Wait, the pipeline steps list "Create PostgreSQL tables" after processing fetched metadata. So the order is: metadata fetching -> download data -> create tables. Or maybe create tables is after download? Let me check the original description:

Under "Pipeline Steps", after the metadata processing (download CSV data), the next step is creating tables. So create tables comes after the download steps. So the create tables ops would depend on the download steps completing. But that might not be correct because creating tables is a prerequisite for loading data. Hmm, perhaps the create tables should happen after the metadata is fetched but before the data is processed. Or maybe the download steps get the data, then create tables, then process and load.

Wait, the steps are:

- Process fetched metadata (download CSV data)
- Create tables
- Parallel data processing (load into Redis, clean plant data)
- Branch based on death data
- Then process death data and power plant data
- Store in PostgreSQL

So the create tables must happen before storing data into them. Therefore, the create_deaths_table and create_power_plants_table ops should be after the download steps but before the steps that load data into PostgreSQL.

So dependencies would be:

fetch_nuclear_metadata → download_nuclear_data

fetch_thermal_metadata → download_thermal_data

fetch_death_resources → download_death_files

fetch_city_geo (no downstream dependency yet?)

Then, after download steps, create tables. So create_deaths_table and create_power_plants_table depend on the download steps? Or maybe they can be created in parallel once the metadata is fetched. Wait, perhaps the create tables don't depend on the downloaded data but are part of the setup. Maybe the create tables can be done after the metadata is fetched but before processing the data. But according to the pipeline steps, create tables come after processing fetched metadata (downloads). So the create tables ops would depend on the download steps.

But logically, you need the tables to exist before inserting data. So the create tables should be done before the data processing steps that load data into them. So the create tables ops should come after the download steps but before the data processing steps that load into PostgreSQL.

Next, parallel data processing: load death records into Redis, clean thermal data, clean nuclear data. These steps can run in parallel. They depend on the download steps and the create tables steps? Or maybe the data processing (cleaning) can happen before the tables are created, but the loading into PostgreSQL would require the tables. So perhaps the create tables ops are dependencies for the ops that store data into PostgreSQL.

Wait, the "Parallel data processing" includes loading death records into Redis, which is an intermediate step. The cleaning of plant data (thermal and nuclear) would prepare the data, and then later steps generate SQL and store into PostgreSQL. So the create tables must be done before the storing steps.

So the create tables ops can be dependencies for the ops that store data into the tables. But the cleaning steps (clean_thermal, clean_nuclear) might not depend on the tables being created, unless the cleaning process requires the table schema. Probably not; the cleaning is just transforming the data. The storing steps (store_power_plants, store_death_records) would need the tables to exist, so those ops would depend on the create tables ops.

So the dependencies would be:

download_death_files → load_death_records_redis

download_thermal_data → clean_thermal_data

download_nuclear_data → clean_nuclear_data

Then, after cleaning, generate SQL and store. Also, the create tables ops (create_deaths_table and create_power_plants_table) must be completed before storing.

But according to the pipeline steps, the create tables are done before the parallel data processing. So the create tables ops are after the download steps and before the data processing steps.

Wait, the pipeline steps list:

- Create PostgreSQL tables (after process fetched metadata)

Then:

- Parallel data processing (load death into Redis, clean thermal, clean nuclear)

So the create tables are done before the data processing steps. Therefore, the data processing steps (load, clean) can run after the create tables. Or maybe not—since loading into Redis doesn't require the tables. So perhaps the create tables are in parallel with the data processing steps? Or maybe the create tables are dependencies for the steps that store into PostgreSQL.

This is getting a bit complicated. Let me outline the dependencies step by step.

Initial steps:

start (if any) → four parallel fetchers (nuclear_meta, thermal_meta, death_resources, city_geo)

Then, each metadata fetch leads to download:

nuclear_meta → download_nuclear

thermal_meta → download_thermal

death_resources → download_death

city_geo → maybe no download step? Or perhaps city_geo is a CSV that's already downloaded, so no further processing?

Then, after downloads, create tables. So create_deaths and create_power_plants depend on download_death, download_thermal, download_nuclear? Or perhaps create tables can be done once the metadata is fetched, but according to the pipeline steps, create tables come after processing fetched metadata (which includes the downloads). So create tables depend on the download steps.

Then, parallel data processing steps:

load_death_redis (depends on download_death)

clean_thermal (depends on download_thermal)

clean_nuclear (depends on download_nuclear)

These three can run in parallel. Then, branching based on death data availability.

The branch is: if death data exists, proceed to cleanse and store; else skip to staging completion.

So after load_death_redis, there's a check. So perhaps an op that checks if death data exists. Let's call it check_death_data. It depends on load_death_redis. Then, based on its output, branch.

In Dagster, conditional branching can be handled using dynamic outputs. However, the user mentioned to model branching using standard Python branching within ops or via fan-in patterns. So perhaps the check_death_data op returns a boolean, and subsequent ops are conditionally executed. But Dagster's execution model is based on static graphs, so dynamic execution (like conditionally running ops) isn't straightforward. However, the user suggests using standard Python branching within ops. So perhaps the ops that follow the check will have conditional logic inside them based on the check's result.

Alternatively, the check_death_data op could emit a value that determines which subsequent ops are executed. But since Dagster's graph is static, the ops must be defined in the graph, but their execution can be skipped based on the output of previous ops. This can be done using a conditional output and using a fan-in pattern.

But perhaps the simplest way, as per the user's instruction, is to handle the branching within an op. For example, the check_death_data op could determine whether death data exists, and then within the subsequent ops (cleanse_death, store_death, clean_death_files), those ops would check if there's data to process. Alternatively, the check_death_data op could return a flag, and the next op (cleanse_death) would only proceed if the flag is True.

But in Dagster, each op is a node in the graph. So if the branching is to have two paths (process death data or skip), the graph must include both paths, but the ops in the 'skip' path would do nothing. Alternatively, the check_death_data op could conditionally execute certain steps within itself, but that's not how ops work. Each op is a separate function.

Hmm. The user's constraint says: "If branching/conditional paths appear, model them using standard Python branching within ops or via fan-in patterns." So perhaps the check_death_data op will return a boolean, and then the next ops (cleanse, store, clean) will take that boolean as input and decide whether to proceed. So those ops would have conditional logic inside them based on the presence of data.

Alternatively, the check_death_data op could output a value that determines whether subsequent ops are executed. But Dagster doesn't support dynamic execution graphs, so all possible ops must be present in the job definition. However, their execution can be conditional based on the output of previous ops. For example, using a conditional output with a DynamicOutput, but that's more complex.

Given the user's instruction to use standard Python branching within ops, perhaps the best approach is to have the check_death_data op return a boolean indicating whether death data exists. Then, the ops that process death data (cleanse_death, store_death, clean_death_files) would take this boolean as an input and only proceed if the boolean is True. Otherwise, they do nothing.

But how to pass the boolean to those ops. Let's see:

check_death_data → returns a boolean.

cleanse_death depends on check_death_data and load_death_records_redis.

Wait, the cleanse_death op needs the death data from Redis. So perhaps:

load_death_records_redis produces the death data, which is passed to check_death_data (which checks if it's non-empty). Then, cleanse_death takes the death data and the check result. If the check result is True, it proceeds; else, it skips.

But in Dagster, each op's inputs are resolved through dependencies. So:

load_death_records_redis → check_death_data (input is the death data)

check_death_data → returns a boolean.

cleanse_death takes the death data and the check result. If check is True, process; else, skip.

But how to structure that. Let's think in code:

@op
def load_death_records_redis(context):
    # load data into Redis, return some reference or data
    return data

@op
def check_death_data(context, data):
    return len(data) > 0

@op
def cleanse_death_records(context, data, has_data):
    if has_data:
        # process data
    else:
        context.log.info("Skipping cleansing as no death data")
    return processed_data

But then, cleanse_death_records would depend on both load_death_records_redis and check_death_data. So in the job, the dependencies would be:

load_death_records_redis → check_death_data

load_death_records_redis → cleanse_death_records (as data input)

check_death_data → cleanse_death_records (as has_data input)

But in Dagster, you can have multiple inputs to an op. So that's possible.

Then, the store_death_records op would depend on cleanse_death_records and create_deaths_table. Similarly, the clean_death_files would depend on cleanse_death_records.

But if has_data is False, cleanse_death_records returns nothing or a None. Then, store_death_records would need to handle that. Alternatively, the ops could be designed to skip processing if data is not present.

Alternatively, the check_death_data op could be responsible for triggering the next steps. But since Dagster's graph is static, all ops must be defined, but their execution can be conditional.

This seems manageable. So the death data processing path would be:

load_death_records_redis → check_death_data → (cleanse_death, which also takes data from load_death_records_redis)

Then, if has_data is True, cleanse proceeds, else skips.

But in code, the cleanse_death op would get the data and the check result, and decide internally whether to process.

Similarly, the store_death_records op would take the processed data from cleanse_death. If cleanse_death didn't process (returned None), then store would do nothing.

But this approach requires that all subsequent ops handle the possibility of missing data.

Alternatively, the check_death_data op could filter the data, passing it forward only if present. But that would require dynamic outputs. For simplicity, perhaps handling within the ops is better, as per the user's instruction.

Now, moving on to the power plant data processing. After cleaning thermal and nuclear data, generate SQL and store into PostgreSQL. So:

clean_thermal_data and clean_nuclear_data → generate_sql (maybe combine the data?) → store_power_plants.

But the pipeline steps mention "Generate SQL queries for plant data persistence" and "Store power plant records in PostgreSQL". So perhaps the clean_thermal and clean_nuclear ops produce cleaned data, which is then combined and used to generate SQL, which is then executed to store.

Alternatively, generate_sql could take both cleaned thermal and nuclear data, create SQL statements, and then store_power_plants executes them.

So dependencies would be:

clean_thermal_data → generate_sql_queries

clean_nuclear_data → generate_sql_queries

generate_sql_queries → store_power_plants

store_power_plants also depends on create_power_plants_table.

Then, the staging completion step (complete_staging) would depend on all paths: store_power_plants, store_death_records (if executed), and the skip path (if death data is missing).

But how to model the fan-in. The complete_staging op would need to depend on both the power plant processing path and the death data path (whether it was executed or skipped). Since all ops are part of the graph, even if they do nothing, their outputs (even if None) would need to be connected.

Alternatively, the complete_staging op could depend on the check_death_data op and the store_power_plants op. But that might not capture all cases.

Alternatively, the death data path after the check includes cleanse_death, store_death, clean_death_files. The complete_staging op would depend on store_power_plants, clean_death_files, and any other completion steps.

But this is getting complex. Let's outline the job structure with all the ops and their dependencies.

Now, considering the constraints:

- Use @op and @job.

- Resources/config: Need to provide stubs. For example, PostgreSQL and Redis resources. These can be defined as commented resources or using default stubs.

- Retry mechanism: The user mentioned retry with 10-second delay. So ops should have a retry policy. In Dagster, this is done via @op(config_schema={...}) or using the retry_policy parameter. So adding a retry policy with delay of 10 seconds and max_retries (but the user says retry mechanism with 10-second delay between attempts; perhaps max_retries=3 or similar, but the user didn't specify. Maybe just a stub comment.)

- Concurrency: Maximum concurrency of 1. In Dagster, this can be set via the executor, e.g., in-process executor with config. But the user's launch pattern includes execute_in_process(), which runs sequentially. So maybe no need to specify, as the default is in-process.

Now, writing the code step by step.

First, import necessary modules:

from dagster import op, job, RetryPolicy

But wait, the user didn't mention specific retry policies, just that retry mechanism exists. So perhaps adding a retry_policy to the ops that need it. For example:

@op(retry_policy=RetryPolicy(max_retries=3, delay=10))

But the user might just want a comment indicating where retries would be applied.

Next, define the ops in order.

Starting with the four fetch ops:

@op
def fetch_nuclear_metadata(context):
    # API call to fetch nuclear power plant metadata
    return metadata

Similarly for fetch_thermal_metadata, fetch_death_resources, fetch_city_geo.

Then, the download ops:

@op
def download_nuclear_data(context, metadata):
    # Use metadata to download CSV
    return data

Similarly for download_thermal_data and download_death_files.

fetch_city_geo may not have a download step, unless the CSV is processed. But according to the pipeline steps, after fetching city geo, it's a CSV, so maybe that's processed later in cleansing death records.

Then, create tables:

@op
def create_deaths_table(context):
    # SQL to create deaths table
    pass

@op
def create_power_plants_table(context):
    # SQL to create power_plants table
    pass

These ops need to run after the download steps. So dependencies would be:

download_nuclear_data → create_power_plants_table?

Wait, no. The create tables are separate. Maybe the create_deaths_table depends on download_death_files, and create_power_plants depends on download_thermal_data and download_nuclear_data. Or perhaps the create tables can be done in parallel once all download steps are done.

But according to the pipeline steps, create tables come after processing fetched metadata (the download steps). So create_deaths_table and create_power_plants_table should depend on the download steps.

But how? For example, create_deaths_table depends on download_death_files, and create_power_plants_table depends on download_thermal_data and download_nuclear_data.

But maybe the create tables can be done in parallel after their respective download steps. For example:

download_death_files → create_deaths_table

download_thermal_data → create_power_plants_table

download_nuclear_data → create_power_plants_table

But that would require create_power_plants_table to depend on both thermal and nuclear downloads. So in Dagster, an op can have multiple upstream dependencies. So:

@op
def create_power_plants_table(context, thermal_data, nuclear_data):
    # create table using thermal and nuclear data (if needed)
    pass

But wait, the thermal_data and nuclear_data would be the downloaded data. But creating the table doesn't require the data itself, just the schema. So perhaps the create_power_plants_table op doesn't need the data, but needs to run after the downloads. So in the job, the create_power_plants_table op depends on download_thermal_data and download_nuclear_data.

Similarly, create_deaths_table depends on download_death_files.

Then, the parallel data processing steps:

load_death_records_redis depends on download_death_files.

clean_thermal_data depends on download_thermal_data.

clean_nuclear_data depends on download_nuclear_data.

These three can run in parallel.

Then, check_death_data depends on load_death_records_redis.

Then, the death data processing ops (cleanse_death_records, store_death_records, clean_death_files) depend on check_death_data and other data.

The power plant processing ops (generate_sql_queries, store_power_plants) depend on clean_thermal_data and clean_nuclear_data, and create_power_plants_table.

Finally, complete_staging depends on store_power_plants and clean_death_files (and possibly other steps).

Putting this all together in the job definition.

Now, considering the job dependencies:

The four fetch ops (nuclear_meta, thermal_meta, death_resources, city_geo) run in parallel.

Then:

nuclear_meta → download_nuclear → create_power_plants_table

thermal_meta → download_thermal → create_power_plants_table

death_resources → download_death → create_deaths_table

city_geo → (no downstream yet?)

Then, create_deaths_table and create_power_plants_table are done.

Then, parallel data processing:

download_death → load_death_redis

download_thermal → clean_thermal

download_nuclear → clean_nuclear

load_death_redis → check_death_data

check_death_data → cleanse_death (which also takes data from load_death_redis)

cleanse_death → store_death (depends on create_deaths_table)

cleanse_death → clean_death_files

clean_thermal and clean_nuclear → generate_sql → store_power_plants (depends on create_power_plants_table)

Finally, complete_staging depends on store_power_plants and clean_death_files.

But how to model the dependencies for complete_staging. It should run after all processing is done, whether death data was processed or not.

So complete_staging would depend on:

- store_power_plants (from power plant processing)

- clean_death_files (from death data processing, which may have been skipped)

But if death data was not present, clean_death_files would still have run (but perhaps done nothing). So in the job, complete_staging must depend on both store_power_plants and clean_death_files.

Additionally, the city_geo data is used in the cleanse_death step for geographic enrichment. So the cleanse_death op would depend on fetch_city_geo.

So, adding that dependency:

fetch_city_geo → cleanse_death_records

Putting all together in the job:

@job
def etl_pipeline():
    nuclear_meta = fetch_nuclear_metadata()
    thermal_meta = fetch_thermal_metadata()
    death_resources = fetch_death_resources()
    city_geo = fetch_city_geo()

    nuclear_data = download_nuclear_data(nuclear_meta)
    thermal_data = download_thermal_data(thermal_meta)
    death_data = download_death_files(death_resources)

    deaths_table = create_deaths_table(death_data)
    power_plants_table = create_power_plants_table(thermal_data, nuclear_data)

    loaded_death = load_death_records_redis(death_data)
    clean_thermal = clean_thermal_data(thermal_data)
    clean_nuclear = clean_nuclear_data(nuclear_data)

    has_data = check_death_data(loaded_death)
    cleansed_death = cleanse_death_records(loaded_death, has_data, city_geo)
    stored_death = store_death_records(cleansed_death, deaths_table)
    cleaned_files = clean_death_files(cleansed_death)

    sql_queries = generate_sql_queries(clean_thermal, clean_nuclear)
    stored_plants = store_power_plants(sql_queries, power_plants_table)

    complete_staging(stored_plants, cleaned_files)

Wait, but in the code above, create_deaths_table depends on death_data (the output of download_death_files), which is correct. Similarly, create_power_plants_table depends on thermal_data and nuclear_data.

But when creating tables, you don't need the data itself, just the schema. So perhaps the create tables ops don't need the data as inputs. Instead, they should run after the download steps. So the dependencies are set by having the create tables ops depend on the download ops, but not consume their outputs.

In Dagster, to set a dependency without passing data, you can use the upstream_depends_on argument in the op decorator, but that's not common. Alternatively, the create tables ops can take the output of the download ops as inputs, even if they don't use them. For example:

@op
def create_deaths_table(context, death_data):
    # death_data is not used, but required for dependency
    # create table here

But that's not ideal. Alternatively, use Dagster's syntax to have the create tables depend on the download steps by using the output of the download steps as inputs, even if they aren't used.

Alternatively, structure the job so that the create_deaths_table op is invoked with the death_data as an input, thus creating a dependency.

Yes, that's what I did in the previous code example. So create_deaths_table takes death_data as input, which ensures it runs after download_death_files.

Similarly, create_power_plants_table takes thermal_data and nuclear_data as inputs, ensuring it runs after both download_thermal_data and download_nuclear_data.

Now, the complete_staging op depends on stored_plants (from store_power_plants) and cleaned_files (from clean_death_files).

But store_power_plants depends on generate_sql_queries, which depends on clean_thermal and clean_nuclear.

clean_thermal depends on thermal_data (from download_thermal_data), which is after thermal_meta.

Now, considering the death data path:

loaded_death is the output of load_death_records_redis, which depends on death_data (from download_death_files).

check_death_data depends on loaded_death.

cleanse_death_records depends on loaded_death, has_data (from check_death_data), and city_geo.

Then, store_death_records depends on cleansed_death and deaths_table (from create_deaths_table).

clean_death_files depends on cleansed_death.

So even if has_data is False, cleanse_death_records would still run but return None or some indicator. Then, store_death_records and clean_death_files would handle that.

But in the code above, all these ops are part of the graph regardless of the check_death_data result. The branching is handled within the ops.

Now, adding resources. The user mentioned PostgreSQL and Redis. So we need to define resources for these. Since the user allows simplified resource examples as comments or stubs, I can add them using the resource system.

For example:

from dagster import resource

@resource
def postgres_db(context):
    # Stub PostgreSQL connection
    return {"conn": "dummy_conn"}

@resource
def redis_client(context):
    # Stub Redis connection
    return {"client": "dummy_redis"}

Then, the ops that interact with these resources would require them. For example, store_death_records and store_power_plants would require the postgres_db resource. load_death_records_redis would require the redis_client resource.

But the user's code must be executable, so including the resource definitions and adding them to the job.

But the user's constraints say to provide simplified resource or config examples as comments or minimal stubs. So perhaps the resources are commented, or included as stubs.

In the code, the resources can be defined, and the job can include a resource_defs parameter.

Putting it all together:

from dagster import op, job, ResourceDefinition, get_dagster_logger

# Define stub resources
postgres_db = ResourceDefinition.mock_resource()
redis_client = ResourceDefinition.mock_resource()

@op(required_resource_keys={"redis"})
def load_death_records_redis(context, death_data):
    logger = get_dagster_logger()
    # Example: context.resources.redis.client.set(...)
    logger.info("Loading death records into Redis")
    return death_data  # Assume returning processed data

@op(required_resource_keys={"postgres"})
def store_death_records(context, cleansed_data, deaths_table):
    logger = get_dagster_logger()
    if cleansed_data is not None:
        # Example: context.resources.postgres.conn.execute(...)
        logger.info("Storing death records into PostgreSQL")
    else:
        logger.info("No death data to store")

@op(required_resource_keys={"postgres"})
def store_power_plants(context, sql_queries, power_plants_table):
    logger = get_dagster_logger()
    # Example: execute SQL queries
    logger.info("Storing power plant data into PostgreSQL")

# Other ops with resource requirements as needed

Then, the job is defined with resource_defs:

@job(
    resource_defs={
        "postgres": postgres_db,
        "redis": redis_client,
    }
)
def etl_pipeline():
    # ... op invocations ...

Now, including all the ops with their dependencies.

Also, adding retry policies where needed. For example, the fetch ops might have retries:

@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_nuclear_metadata(context):
    # API call
    return {}

But the user mentioned retry mechanism with 10-second delay. So adding retry_policy=RetryPolicy(max_retries=3, delay=10) to the fetch and download ops.

Putting all together, the final code would look like this.
</think>

from dagster import op, job, ResourceDefinition, get_dagster_logger, RetryPolicy

# --- Resources ---
postgres_db = ResourceDefinition.mock_resource()
redis_client = ResourceDefinition.mock_resource()

# --- Ops ---
@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_nuclear_metadata():
    logger = get_dagster_logger()
    logger.info("Fetching nuclear power plant metadata")
    return {"metadata": "nuclear_meta"}

@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_thermal_metadata():
    logger = get_dagster_logger()
    logger.info("Fetching thermal power plant metadata")
    return {"metadata": "thermal_meta"}

@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_death_resources():
    logger = get_dagster_logger()
    logger.info("Fetching death records resource list")
    return {"resources": "death_resources"}

@op(retry_policy=RetryPolicy(max_retries=3, delay=10))
def fetch_city_geo():
    logger = get_dagster_logger()
    logger.info("Fetching city geographic coordinates")
    return {"data": "city_geo"}

@op
def download_nuclear_data(nuclear_metadata):
    logger = get_dagster_logger()
    logger.info("Downloading nuclear plant CSV data")
    return {"nuclear_data": "sample"}

@op
def download_thermal_data(thermal_metadata):
    logger = get_dagster_logger()
    logger.info("Downloading thermal plant CSV data")
    return {"thermal_data": "sample"}

@op
def download_death_files(death_resources):
    logger = get_dagster_logger()
    logger.info("Downloading death record files")
    return {"death_data": "sample"}

@op
def create_deaths_table(_):
    logger = get_dagster_logger()
    logger.info("Creating deaths table in PostgreSQL")

@op
def create_power_plants_table(_):
    logger = get_dagster_logger()
    logger.info("Creating power_plants table in PostgreSQL")

@op(required_resource_keys={"redis"})
def load_death_records_redis(_, death_data):
    logger = get_dagster_logger()
    logger.info("Loading death records into Redis")
    return death_data

@op
def clean_thermal_data(_, thermal_data):
    logger = get_dagster_logger()
    logger.info("Cleaning thermal plant data")
    return thermal_data

@op
def clean_nuclear_data(_, nuclear_data):
    logger = get_dagster_logger()
    logger.info("Cleaning nuclear plant data")
    return nuclear_data

@op
def check_death_data(_, death_records):
    logger = get_dagster_logger()
    has_data = bool(death_records.get("death_data"))
    logger.info(f"Death data available: {has_data}")
    return has_data

@op
def cleanse_death_records(_, death_records, has_data, city_geo):
    logger = get_dagster_logger()
    if has_data:
        logger.info("Cleansing death records with city geo data")
        return {"cleansed_data": "sample"}
    logger.info("Skipping death data cleansing")
    return None

@op(required_resource_keys={"postgres"})
def store_death_records(_, cleansed_data, _deaths_table):
    logger = get_dagster_logger()
    if cleansed_data:
        logger.info("Storing cleansed death records")

@op
def clean_death_files(_, cleansed_data):
    logger = get_dagster_logger()
    if cleansed_data:
        logger.info("Cleaning temporary death data files")
    else:
        logger.info("No death files to clean")

@op
def generate_sql_queries(_, thermal_data, nuclear_data):
    logger = get_dagster_logger()
    logger.info("Generating SQL queries for plant data")
    return {"queries": "sample"}

@op(required_resource_keys={"postgres"})
def store_power_plants(_, sql_queries, _power_plants_table):
    logger = get_dagster_logger()
    logger.info("Storing power plant records in PostgreSQL")

@op
def complete_staging(_):
    logger = get_dagster_logger()
    logger.info("Staging phase completed")

# --- Job ---
@job(
    resource_defs={
        "postgres": postgres_db,
        "redis": redis_client,
    }
)
def etl_pipeline():
    city_geo = fetch_city_geo()
    
    nuclear_data = download_nuclear_data(fetch_nuclear_metadata())
    thermal_data = download_thermal_data(fetch_thermal_metadata())
    death_data = download_death_files(fetch_death_resources())
    
    deaths_table = create_deaths_table(death_data)
    plants_table = create_power_plants_table(thermal_data, nuclear_data)
    
    loaded_death = load_death_records_redis(death_data)
    clean_therm = clean_thermal_data(thermal_data)
    clean_nucl = clean_nuclear_data(nuclear_data)
    
    has_data = check_death_data(loaded_death)
    cleansed = cleanse_death_records(loaded_death, has_data, city_geo)
    store_death = store_death_records(cleansed, deaths_table)
    clean_files = clean_death_files(cleansed)
    
    sql = generate_sql_queries(clean_therm, clean_nucl)
    store_plants = store_power_plants(sql, plants_table)
    
    complete_staging(store_plants, clean_files)

if __name__ == "__main__":
    result = etl_pipeline.execute_in_process()