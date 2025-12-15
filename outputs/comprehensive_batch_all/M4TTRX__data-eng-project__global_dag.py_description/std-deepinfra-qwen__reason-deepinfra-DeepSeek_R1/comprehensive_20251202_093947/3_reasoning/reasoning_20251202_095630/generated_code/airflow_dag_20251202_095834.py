<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline involves multiple steps: data ingestion, processing, branching, and loading into PostgreSQL. The user specified using Airflow 2.x style imports, so I'll make sure to use the correct modules like PythonOperator, DAG, and any necessary sensors. Wait, the description doesn't mention sensors, so maybe I don't need them here. But there's conditional branching based on data availability, so I'll need a BranchPythonOperator.

The DAG should have manual triggers only, so schedule_interval is None. Also, catchup should be False. Default args need to include things like retries, retry_delay, and maybe other parameters like start_date. The user mentioned a maximum concurrency of 1, so I'll set concurrency in default_args.

The pipeline starts with parallel data ingestion from four sources. So, four tasks running in parallel. Then, processing the fetched metadata, which probably depends on the ingestion tasks. Then creating PostgreSQL tables. Wait, the steps mention creating tables after processing metadata. Wait, the steps are:

- Start
- Parallel ingestion (four tasks)
- Process fetched metadata (download CSV/data files)
- Create tables
- Parallel data processing (three tasks: load death to Redis, clean thermal, clean nuclear)
- Branch based on death data availability
- Then either process death data or skip to staging
- Power plant processing
- Complete staging

So dependencies need to be set up correctly. Let's outline the tasks:

1. Start pipeline execution: Maybe a DummyOperator as a starting point.

2. Parallel ingestion tasks:
   - fetch_nuclear_metadata
   - fetch_thermal_metadata
   - fetch_death_resources
   - fetch_city_geo

These four tasks can run in parallel after the start.

Then, process fetched metadata, which includes downloading CSV data. So perhaps:

3. download_nuclear_csv (depends on fetch_nuclear_metadata)
4. download_thermal_csv (depends on fetch_thermal_metadata)
5. download_death_files (depends on fetch_death_resources)

These three tasks would run after their respective fetch tasks. But since the four initial tasks are parallel, these three would be downstream of each of their parents.

Then create PostgreSQL tables: create_deaths_table and create_power_plants_table. These probably need to happen after the download tasks, because the tables need to exist before loading data. Wait, the steps say "Create PostgreSQL tables" comes after processing fetched metadata. So maybe the create tables tasks depend on the download tasks.

Then parallel data processing: load_death_to_redis, clean_thermal_data, clean_nuclear_data. These three tasks would depend on the create tables tasks and the download tasks? Or maybe the create tables tasks need to be completed before data processing can start.

Wait, the steps are:

After processing metadata (downloads), create tables. Then parallel data processing. So create tables must come after the downloads. So dependencies would be:

download_nuclear_csv, download_thermal_csv, download_death_files >> create_deaths_table and create_power_plants_table? Or maybe the create tables can be done in parallel with the downloads? No, because the downloads are part of processing fetched metadata. Wait, the pipeline steps list "Create PostgreSQL tables" as a separate step after processing metadata. So the create tables tasks must come after the download tasks.

So after download tasks complete, then create tables. Then the parallel data processing tasks (load death, clean thermal, clean nuclear) can start, which depend on the tables being created.

Then branching based on death data availability. So after load_death_to_redis, check if data exists. If yes, proceed with cleansing and storing death records. If not, skip to staging completion.

The BranchPythonOperator would decide which path to take. Let's call this task check_death_data. Then, the death data processing path includes cleanse_death_records, store_death_records, clean_temp_death_files. The other path is skip_staging, which is a DummyOperator leading to complete_staging.

Then power plant processing: generate_sql_queries and store_power_plants. These tasks might depend on the clean_thermal_data and clean_nuclear_data tasks.

Finally, complete staging phase, which depends on both the death data path (if taken) and the power plant processing, as well as the skip path.

Now, organizing this into task groups. The user mentioned using task groups for logical grouping of ingestion and staging phases. So perhaps group the ingestion tasks (the four fetch tasks and their download steps) into a task group. Then staging might include the create tables and processing steps.

Wait, the pipeline steps mention "task groups for logical grouping of ingestion and staging phases". So maybe:

- ingestion group: includes the four fetch tasks and the three download tasks.
- staging group: includes create tables, data processing tasks, branching, etc.

But I need to structure the code with TaskGroup.

Now, default_args should include retries (the user mentioned retry mechanism with 10-second delay), so retries=3 (or some number), retry_delay=timedelta(seconds=10). Also, start_date is required for the DAG. The user didn't specify, so I'll set a default start_date, maybe days_ago(1).

Concurrency is set to 1 via default_args['concurrency'] = 1.

Now, writing the code:

Import necessary modules: DAG, PythonOperator, TaskGroup, BranchPythonOperator, etc. Also, datetime and timedelta.

Define default_args with owner, depends_on_past, email, etc., as per best practices. Include retries and retry_delay.

Instantiate the DAG with dag_id, default_args, schedule_interval=None, catchup=False, tags, etc.

Then, define the tasks. Start with a dummy start task.

Then, the four parallel fetch tasks. Each is a PythonOperator calling a function (like fetch_nuclear_metadata). These functions are placeholders, but the user said to avoid TODOs, so maybe define them with pass, but in practice, they would have API calls.

Then, the download tasks depend on their respective fetch tasks. For example, download_nuclear_csv depends on fetch_nuclear_metadata.

Then, create tables tasks. These depend on all download tasks. So download_nuclear_csv, download_thermal_csv, download_death_files must complete before create_deaths_table and create_power_plants_table can run. Wait, maybe create_deaths_table depends on download_death_files, and create_power_plants_table depends on download_nuclear and thermal? Or perhaps both tables are created after all downloads are done. The pipeline steps say "Create PostgreSQL tables" comes after processing fetched metadata (the downloads), so all downloads must complete before creating tables.

So, the create tables tasks should depend on download_nuclear_csv, download_thermal_csv, and download_death_files. So after all three download tasks complete, then create tables.

But how to set dependencies? Since download_nuclear_csv and download_thermal_csv depend on their respective fetch tasks, and download_death_files depends on fetch_death_resources, the three download tasks are downstream of their respective fetch tasks. Then, create_deaths_table and create_power_plants_table must be downstream of all three download tasks.

So:

[download_nuclear_csv, download_thermal_csv, download_death_files] >> create_deaths_table

[download_nuclear_csv, download_thermal_csv, download_death_files] >> create_power_plants_table

But maybe each create table task depends on different download tasks. For example, create_deaths_table depends on download_death_files, and create_power_plants depends on download_nuclear and thermal. But according to the pipeline steps, both tables are created after processing metadata, which includes all downloads. So perhaps the create tables tasks depend on all download tasks. Alternatively, maybe create_deaths_table depends on download_death_files, and create_power_plants depends on download_nuclear and thermal. Let me check the pipeline steps again.

The pipeline steps list "Create PostgreSQL tables" as a separate step after processing fetched metadata. Processing fetched metadata includes downloading CSV/data files. So after all downloads, create both tables. So the create tables tasks should depend on all three download tasks. So:

download_nuclear_csv >> create_power_plants_table

download_thermal_csv >> create_power_plants_table

download_death_files >> create_deaths_table

Wait, maybe the power_plants table combines nuclear and thermal data, so create_power_plants_table depends on both download_nuclear_csv and download_thermal_csv. Whereas create_deaths_table depends on download_death_files.

But according to the pipeline steps, the create tables step is after processing metadata (downloads), so perhaps both tables are created after all downloads. But maybe the deaths table only needs the death data download. So perhaps:

download_death_files >> create_deaths_table

download_nuclear_csv >> create_power_plants_table

download_thermal_csv >> create_power_plants_table

But that would allow create_deaths_table to run once download_death_files is done, and create_power_plants_table to run once both nuclear and thermal downloads are done. But according to the pipeline steps, the create tables step is a single step, but in reality, there are two tables. So perhaps the two create table tasks can run in parallel once their respective dependencies are met.

But the pipeline steps say "Create PostgreSQL tables" as a step, which includes creating both tables. So maybe the create tables tasks can run in parallel once their respective download tasks are done. So:

download_death_files >> create_deaths_table

download_nuclear_csv >> create_power_plants_table

download_thermal_csv >> create_power_plants_table

But then create_power_plants_table would depend on both download_nuclear_csv and download_thermal_csv? Or perhaps each download triggers their own create table? No, the power_plants table probably combines both nuclear and thermal data, so the create table should happen after both are downloaded. So create_power_plants_table depends on both download_nuclear_csv and download_thermal_csv.

So:

download_nuclear_csv >> create_power_plants_table

download_thermal_csv >> create_power_plants_table

download_death_files >> create_deaths_table

Then, the parallel data processing tasks (load_death_to_redis, clean_thermal_data, clean_nuclear_data) depend on the create tables tasks. For example:

create_deaths_table >> load_death_to_redis

create_power_plants_table >> clean_thermal_data

create_power_plants_table >> clean_nuclear_data

But the pipeline steps say "Parallel data processing" includes loading death records into Redis, cleaning thermal and nuclear data. These tasks can run in parallel once their respective dependencies are met. So load_death_to_redis depends on create_deaths_table, clean_thermal_data depends on create_power_plants_table, and clean_nuclear_data depends on create_power_plants_table.

Then, after these three tasks, the branching occurs. The check_death_data task depends on load_death_to_redis. Because the branching is based on death data availability, which is loaded into Redis. So:

load_death_to_redis >> check_death_data

The check_death_data is a BranchPythonOperator that decides whether to proceed with the death data processing or skip. The possible branches are:

- cleanse_death_records (if data exists)
- skip_staging (if no data)

Then, the death data processing path:

check_death_data >> cleanse_death_records >> store_death_records >> clean_temp_death_files

And the skip path:

check_death_data >> skip_staging

Then, the power plant processing tasks (generate_sql_queries and store_power_plants) depend on clean_thermal_data and clean_nuclear_data. So:

clean_thermal_data >> generate_sql_queries

clean_nuclear_data >> generate_sql_queries

generate_sql_queries >> store_power_plants

Finally, the complete_staging task depends on:

- clean_temp_death_files (from death data path)
- store_power_plants (from power plant path)
- skip_staging (if the death data was skipped)

So, we need to set dependencies such that complete_staging runs after all possible paths. So:

[clean_temp_death_files, store_power_plants, skip_staging] >> complete_staging

Putting all together:

start >> [fetch_nuclear_metadata, fetch_thermal_metadata, fetch_death_resources, fetch_city_geo]

fetch_nuclear_metadata >> download_nuclear_csv

fetch_thermal_metadata >> download_thermal_csv

fetch_death_resources >> download_death_files

download_nuclear_csv >> create_power_plants_table

download_thermal_csv >> create_power_plants_table

download_death_files >> create_deaths_table

create_deaths_table >> load_death_to_redis

create_power_plants_table >> clean_thermal_data

create_power_plants_table >> clean_nuclear_data

load_death_to_redis >> check_death_data

clean_thermal_data >> generate_sql_queries

clean_nuclear_data >> generate_sql_queries

generate_sql_queries >> store_power_plants

check_death_data >> [cleanse_death_records, skip_staging]

cleanse_death_records >> store_death_records >> clean_temp_death_files

[clean_temp_death_files, store_power_plants, skip_staging] >> complete_staging

Now, implementing this in code with TaskGroups. The user mentioned using task groups for ingestion and staging phases. So:

- ingestion_group: includes the four fetch tasks and three download tasks.
- staging_group: includes create tables, data processing, branching, etc.

But maybe the ingestion group is the initial parallel fetches and their downloads. Then staging includes the rest. Alternatively, the task groups can be organized as per the pipeline's logical phases.

But the code needs to structure the tasks with TaskGroup. Let's create two task groups: one for ingestion and one for staging.

Wait, the pipeline steps mention "task groups for logical grouping of ingestion and staging phases". So:

- ingestion_phase: includes the four fetch tasks and their download tasks.
- staging_phase: includes create tables, data processing, branching, etc.

But how to structure this. Let's see:

Within the DAG context:

with DAG(...) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='ingestion_phase') as ingestion_group:
        # Define the four fetch tasks and three download tasks here
        fetch_nuclear_metadata = PythonOperator(...)
        fetch_thermal_metadata = PythonOperator(...)
        fetch_death_resources = PythonOperator(...)
        fetch_city_geo = PythonOperator(...)

        download_nuclear_csv = PythonOperator(...)
        download_thermal_csv = PythonOperator(...)
        download_death_files = PythonOperator(...)

        # Set dependencies within the task group
        fetch_nuclear_metadata >> download_nuclear_csv
        fetch_thermal_metadata >> download_thermal_csv
        fetch_death_resources >> download_death_files
        # fetch_city_geo has no download task? Because the pipeline steps mention four sources, but the processing steps include downloading CSV/data for nuclear, thermal, death, but city_geo is a CSV fetch. Maybe the city_geo is part of the initial fetch and doesn't require further download. So perhaps the download_death_files handles that. Wait, the pipeline steps under "Process fetched metadata" include downloading death record files, which was fetched in the death resources. So maybe the fetch_city_geo is a CSV that's already downloaded, so no further processing? Or perhaps the city_geo is used in cleansing death records. So maybe the download_death_files is part of processing death resources, but the city_geo CSV is fetched and used as-is. So the fetch_city_geo task would download the CSV, and that's used later in cleansing death records.

But according to the pipeline steps, under "Process fetched metadata", the steps are:

- Download nuclear power plant CSV data

- Download thermal power plant CSV data

- Download death record files

So the city_geo CSV is fetched in the initial parallel ingestion but doesn't require further processing. So the fetch_city_geo task is part of the initial four parallel tasks, and the CSV is available after that. So in the code, fetch_city_geo is a PythonOperator that fetches the CSV, and that's it. Then, later steps (like cleansing death records) would use that CSV.

So in the ingestion group:

The four fetch tasks (including fetch_city_geo) are all part of the initial parallel tasks. Then, three download tasks (nuclear, thermal, death) depend on their respective fetch tasks. The fetch_city_geo has no download task, so it's part of the initial four, but its data is used later.

So dependencies:

start >> ingestion_group (which includes the four fetch tasks and three download tasks, with dependencies between fetch and download tasks). But how to structure the task group. The fetch_city_geo task is part of the ingestion group but doesn't have a download task. So within the ingestion_group:

fetch_nuclear_metadata >> download_nuclear_csv

fetch_thermal_metadata >> download_thermal_csv

fetch_death_resources >> download_death_files

fetch_city_geo has no downstream tasks in the ingestion group.

Then, outside the ingestion group, the create tables tasks depend on the download tasks. But since the download tasks are inside the ingestion_group, how to reference them. Maybe after the ingestion_group, the download tasks are children of the group. For example, the download_nuclear_csv task's ID would be 'ingestion_phase.download_nuclear_csv' if using TaskGroup. So when setting dependencies, after the ingestion_group, the create_power_plants_table depends on ingestion_group's download_nuclear_csv and download_thermal_csv.

But perhaps structuring the code with TaskGroup isn't necessary here. Alternatively, maybe the user intended to group the initial four fetch tasks and their download tasks into a task group called 'ingestion_phase', and the rest into 'staging_phase'. But given the complexity, perhaps using TaskGroup for ingestion_phase and staging_phase would help organize the code.

Alternatively, maybe the initial four fetch tasks are in a task group, and the download tasks are in another. But the pipeline steps mention "task groups for logical grouping of ingestion and staging phases". So ingestion_phase includes fetching and downloading, staging includes the rest.

But perhaps the initial four fetch tasks are parallel, then their download tasks are sequential after each. Then, the create tables, etc., are part of staging.

But regardless, the code must be structured correctly.

Now, writing the code:

Import TaskGroup from airflow.utils.task_group.

Then, within the DAG:

with DAG(...) as dag:

    start_op = DummyOperator(task_id='start')

    with TaskGroup(group_id='ingestion_phase') as ingestion_group:
        fetch_nuclear_metadata = PythonOperator(
            task_id='fetch_nuclear_metadata',
            python_callable=fetch_nuclear_metadata_func,
        )
        fetch_thermal_metadata = PythonOperator(
            task_id='fetch_thermal_metadata',
            python_callable=fetch_thermal_metadata_func,
        )
        fetch_death_resources = PythonOperator(
            task_id='fetch_death_resources',
            python_callable=fetch_death_resources_func,
        )
        fetch_city_geo = PythonOperator(
            task_id='fetch_city_geo',
            python_callable=fetch_city_geo_func,
        )

        download_nuclear_csv = PythonOperator(
            task_id='download_nuclear_csv',
            python_callable=download_nuclear_csv_func,
        )
        download_thermal_csv = PythonOperator(
            task_id='download_thermal_csv',
            python_callable=download_thermal_csv_func,
        )
        download_death_files = PythonOperator(
            task_id='download_death_files',
            python_callable=download_death_files_func,
        )

        # Set dependencies within ingestion group
        fetch_nuclear_metadata >> download_nuclear_csv
        fetch_thermal_metadata >> download_thermal_csv
        fetch_death_resources >> download_death_files
        # fetch_city_geo has no download task

    # Staging phase tasks
    with TaskGroup(group_id='staging_phase') as staging_group:
        create_deaths_table = PythonOperator(
            task_id='create_deaths_table',
            python_callable=create_deaths_table_func,
        )
        create_power_plants_table = PythonOperator(
            task_id='create_power_plants_table',
            python_callable=create_power_plants_table_func,
        )

        # Data processing tasks
        load_death_to_redis = PythonOperator(
            task_id='load_death_to_redis',
            python_callable=load_death_to_redis_func,
        )
        clean_thermal_data = PythonOperator(
            task_id='clean_thermal_data',
            python_callable=clean_thermal_data_func,
        )
        clean_nuclear_data = PythonOperator(
            task_id='clean_nuclear_data',
            python_callable=clean_nuclear_data_func,
        )

        # Branching
        check_death_data = BranchPythonOperator(
            task_id='check_death_data',
            python_callable=check_death_data_func,
        )
        cleanse_death_records = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=cleanse_death_records_func,
        )
        store_death_records = PythonOperator(
            task_id='store_death_records',
            python_callable=store_death_records_func,
        )
        clean_temp_death_files = PythonOperator(
            task_id='clean_temp_death_files',
            python_callable=clean_temp_death_files_func,
        )
        skip_staging = DummyOperator(
            task_id='skip_staging'
        )

        # Power plant processing
        generate_sql_queries = PythonOperator(
            task_id='generate_sql_queries',
            python_callable=generate_sql_queries_func,
        )
        store_power_plants = PythonOperator(
            task_id='store_power_plants',
            python_callable=store_power_plants_func,
        )

        complete_staging = DummyOperator(
            task_id='complete_staging'
        )

        # Dependencies within staging group
        # Create tables depend on download tasks from ingestion group
        [ingestion_group.download_nuclear_csv, ingestion_group.download_thermal_csv] >> create_power_plants_table
        ingestion_group.download_death_files >> create_deaths_table

        create_deaths_table >> load_death_to_redis
        create_power_plants_table >> [clean_thermal_data, clean_nuclear_data]

        load_death_to_redis >> check_death_data
        check_death_data >> [cleanse_death_records, skip_staging]

        cleanse_death_records >> store_death_records >> clean_temp_death_files

        [clean_thermal_data, clean_nuclear_data] >> generate_sql_queries
        generate_sql_queries >> store_power_plants

        [clean_temp_death_files, store_power_plants, skip_staging] >> complete_staging

    # Set dependencies between start, ingestion, and staging
    start_op >> ingestion_group
    ingestion_group >> staging_group

Wait, but task groups can't be directly connected like that. Instead, dependencies are set between tasks inside and outside the groups. So after the ingestion_group, the create_power_plants_table and create_deaths_table depend on the download tasks inside the ingestion_group.

But when using TaskGroup, the task IDs are prefixed. For example, the download_nuclear_csv task inside the ingestion_group would have the task_id 'ingestion_phase.download_nuclear_csv'. So when referencing them outside the group, we need to use that ID.

Alternatively, perhaps the create tables are part of the staging_group, and their dependencies are set on the download tasks from the ingestion_group.

So, inside the staging_group's dependencies:

download_nuclear_csv = ingestion_group.download_nuclear_csv

download_thermal_csv = ingestion_group.download_thermal_csv

download_death_files = ingestion_group.download_death_files

Then:

[download_nuclear_csv, download_thermal_csv] >> create_power_plants_table

download_death_files >> create_deaths_table

But how to reference these tasks from inside the staging_group. Maybe the code should structure the dependencies correctly.

Alternatively, perhaps the task groups are not necessary, and the code can be written without them, but the user mentioned using task groups. So I need to include them.

Alternatively, perhaps the ingestion_group includes the four fetch tasks and the three download tasks, and the dependencies are set within the group. Then, the create tables tasks are outside the group but depend on the download tasks inside the group.

But in Airflow, when you have a task group, the tasks inside are organized under the group, but their task_ids are accessible via the group's ID.

But perhaps it's easier to structure the code without task groups for simplicity, but the user specified to use them. So I'll proceed.

Another approach: the initial four fetch tasks are in a parallel group, then the three download tasks are sequential after their respective fetch tasks. Then, the create tables tasks depend on the download tasks.

But perhaps using TaskGroup for the ingestion phase (all fetch and download tasks) and another for staging phase (the rest).

But in code:

with DAG(...) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='ingestion_phase') as ingestion_group:
        fetch_nuclear = PythonOperator(task_id='fetch_nuclear_metadata', ...)
        fetch_thermal = PythonOperator(task_id='fetch_thermal_metadata', ...)
        fetch_death = PythonOperator(task_id='fetch_death_resources', ...)
        fetch_city = PythonOperator(task_id='fetch_city_geo', ...)

        download_nuclear = PythonOperator(task_id='download_nuclear_csv', ...)
        download_thermal = PythonOperator(task_id='download_thermal_csv', ...)
        download_death = PythonOperator(task_id='download_death_files', ...)

        fetch_nuclear >> download_nuclear
        fetch_thermal >> download_thermal
        fetch_death >> download_death
        # fetch_city has no download task

    # Then, create tables tasks outside the ingestion group
    create_deaths_table = PythonOperator(task_id='create_deaths_table', ...)
    create_power_plants_table = PythonOperator(task_id='create_power_plants_table', ...)

    # Set dependencies: create_deaths_table depends on download_death
    ingestion_group.download_death >> create_deaths_table
    ingestion_group.download_nuclear >> create_power_plants_table
    ingestion_group.download_thermal >> create_power_plants_table

    # Then staging phase tasks
    with TaskGroup(group_id='staging_phase') as staging_group:
        # Define all staging tasks here
        load_death = PythonOperator(...)
        clean_thermal = PythonOperator(...)
        clean_nuclear = PythonOperator(...)
        check_data = BranchPythonOperator(...)
        # etc.

        # Dependencies within staging group
        create_deaths_table >> load_death
        create_power_plants_table >> [clean_thermal, clean_nuclear]
        # ... other dependencies

    # Then dependencies between ingestion and staging
    start >> ingestion_group
    [create_deaths_table, create_power_plants_table] >> staging_group

But this might not be the correct way to structure dependencies between task groups. Alternatively, the create tables tasks are part of the staging group.

Alternatively, perhaps the staging group includes all tasks after the ingestion phase. So the create tables, data processing, branching, etc., are all inside the staging_group.

In that case:

start >> ingestion_group

ingestion_group >> staging_group

But inside the staging_group, the create tables depend on the download tasks from the ingestion_group.

But how to reference tasks from another task group. For example, inside the staging_group, the create_deaths_table depends on ingestion_group's download_death_files task.

In code, this would be:

ingestion_group_download_death = ingestion_group.get_child_task('download_death_files')

But in Airflow, you can reference tasks by their task_id, which includes the group hierarchy. So if the download_death_files task is inside the ingestion_phase group, its task_id is 'ingestion_phase.download_death_files'.

So within the staging_group's dependencies:

create_deaths_table = PythonOperator(...)

create_deaths_table << dag.get_task('ingestion_phase.download_death_files')

But perhaps it's easier to reference them directly via their task_id.

Alternatively, when defining the tasks inside the ingestion_group, we can assign them to variables and then use them outside.

But when using TaskGroup, the tasks inside are part of the group's context, and their IDs are prefixed. So when setting dependencies outside the group, you can refer to them as, e.g., ingestion_group['download_death_files'].

But perhaps the correct way is to reference the tasks by their IDs.

Alternatively, perhaps the code can be structured without using TaskGroup, but the user mentioned to use them. So I'll proceed with TaskGroup.

Putting it all together, the code would look like:

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2023, 1, 1),
    'concurrency': 1,
}

def fetch_nuclear_metadata_func(**kwargs):
    pass

def fetch_thermal_metadata_func(**kwargs):
    pass

def fetch_death_resources_func(**kwargs):
    pass

def fetch_city_geo_func(**kwargs):
    pass

def download_nuclear_csv_func(**kwargs):
    pass

def download_thermal_csv_func(**kwargs):
    pass

def download_death_files_func(**kwargs):
    pass

def create_deaths_table_func(**kwargs):
    pass

def create_power_plants_table_func(**kwargs):
    pass

def load_death_to_redis_func(**kwargs):
    pass

def clean_thermal_data_func(**kwargs):
    pass

def clean_nuclear_data_func(**kwargs):
    pass

def check_death_data_func(**kwargs):
    # Logic to check if death data exists
    return 'cleanse_death_records'  # or 'skip_staging' based on data

def cleanse_death_records_func(**kwargs):
    pass

def store_death_records_func(**kwargs):
    pass

def clean_temp_death_files_func(**kwargs):
    pass

def generate_sql_queries_func(**kwargs):
    pass

def store_power_plants_func(**kwargs):
    pass

with DAG(
    dag_id='french_govt_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['government_data'],
) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='ingestion_phase') as ingestion_group:
        fetch_nuclear_metadata = PythonOperator(
            task_id='fetch_nuclear_metadata',
            python_callable=fetch_nuclear_metadata_func,
        )
        fetch_thermal_metadata = PythonOperator(
            task_id='fetch_thermal_metadata',
            python_callable=fetch_thermal_metadata_func,
        )
        fetch_death_resources = PythonOperator(
            task_id='fetch_death_resources',
            python_callable=fetch_death_resources_func,
        )
        fetch_city_geo = PythonOperator(
            task_id='fetch_city_geo',
            python_callable=fetch_city_geo_func,
        )

        download_nuclear_csv = PythonOperator(
            task_id='download_nuclear_csv',
            python_callable=download_nuclear_csv_func,
        )
        download_thermal_csv = PythonOperator(
            task_id='download_thermal_csv',
            python_callable=download_thermal_csv_func,
        )
        download_death_files = PythonOperator(
            task_id='download_death_files',
            python_callable=download_death_files_func,
        )

        fetch_nuclear_metadata >> download_nuclear_csv
        fetch_thermal_metadata >> download_thermal_csv
        fetch_death_resources >> download_death_files

    with TaskGroup(group_id='staging_phase') as staging_group:
        create_deaths_table = PythonOperator(
            task_id='create_deaths_table',
            python_callable=create_deaths_table_func,
        )
        create_power_plants_table = PythonOperator(
            task_id='create_power_plants_table',
            python_callable=create_power_plants_table_func,
        )

        load_death_to_redis = PythonOperator(
            task_id='load_death_to_redis',
            python_callable=load_death_to_redis_func,
        )
        clean_thermal_data = PythonOperator(
            task_id='clean_thermal_data',
            python_callable=clean_thermal_data_func,
        )
        clean_nuclear_data = PythonOperator(
            task_id='clean_nuclear_data',
            python_callable=clean_nuclear_data_func,
        )

        check_death_data = BranchPythonOperator(
            task_id='check_death_data',
            python_callable=check_death_data_func,
        )
        cleanse_death_records = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=cleanse_death_records_func,
        )
        store_death_records = PythonOperator(
            task_id='store_death_records',
            python_callable=store_death_records_func,
        )
        clean_temp_death_files = PythonOperator(
            task_id='clean_temp_death_files',
            python_callable=clean_temp_death_files_func,
        )
        skip_staging = DummyOperator(task_id='skip_staging')

        generate_sql_queries = PythonOperator(
            task_id='generate_sql_queries',
            python_callable=generate_sql_queries_func,
        )
        store_power_plants = PythonOperator(
            task_id='store_power_plants',
            python_callable=store_power_plants_func,
        )

        complete_staging = DummyOperator(task_id='complete_staging')

        # Dependencies within staging_group
        # Create tables depend on download tasks from ingestion_group
        ingestion_group.download_death_files >> create_deaths_table
        [ingestion_group.download_nuclear_csv, ingestion_group.download_thermal_csv] >> create_power_plants_table

        create_deaths_table >> load_death_to_redis
        create_power_plants_table >> [clean_thermal_data, clean_nuclear_data]

        load_death_to_redis >> check_death_data
        check_death_data >> [cleanse_death_records, skip_staging]

        cleanse_death_records >> store_death_records >> clean_temp_death_files

        [clean_thermal_data, clean_nuclear_data] >> generate_sql_queries
        generate_sql_queries >> store_power_plants

        [clean_temp_death_files, store_power_plants, skip_staging] >> complete_staging

    # Set dependencies between start, ingestion_group, and staging_group
    start >> ingestion_group
    ingestion_group >> staging_group

Wait, but 'ingestion_group >> staging_group' implies that all tasks in ingestion_group must complete before any task in staging_group starts. But in reality, the create_deaths_table and create_power_plants_table depend on specific download tasks, not all ingestion tasks. So perhaps the dependencies should be set explicitly.

But the code above uses 'ingestion_group >> staging_group', which would mean that all tasks in the ingestion_group (the four fetch tasks and three download tasks) must complete before any task in the staging_group starts. However, the create_deaths_table depends only on download_death_files, and create_power_plants_table depends on download_nuclear_csv and download_thermal_csv. The fetch_city_geo task doesn't have any downstream tasks in the staging_group, but according to the pipeline steps, it's part of the initial ingestion and its data is used later. So perhaps the fetch_city_geo task's data is used in the cleanse_death_records step, which is part of the staging_group. Therefore, the cleanse_death_records task depends on fetch_city_geo.

But in the current setup, the staging_group's tasks depend only on the create tables and other tasks within the staging_group. The fetch_city_geo task is part of the ingestion_group, but there's no explicit dependency between it and the cleanse_death_records task. This is a problem because the cleanse_death_records step needs the city_geo data fetched in the ingestion phase.

So, the code as