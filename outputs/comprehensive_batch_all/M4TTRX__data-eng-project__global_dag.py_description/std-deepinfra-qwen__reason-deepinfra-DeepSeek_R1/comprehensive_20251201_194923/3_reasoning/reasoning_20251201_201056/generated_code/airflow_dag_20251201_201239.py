<think>
Okay, I need to convert the given pipeline description into an Apache Airflow DAG. Let me start by understanding the requirements and constraints.

First, the pipeline has several steps: data ingestion from four sources in parallel, processing metadata, creating PostgreSQL tables, parallel data processing, branching based on data availability, and then different paths for death data and power plant data. The orchestration requires manual triggers, max concurrency of 1, retries, and task groups for grouping.

I'll start by setting up the DAG with the specified parameters: schedule_interval is None because it's manual, catchup=False, default_args including retries and retry_delay. The concurrency should be 1.

Next, the tasks. The first step is the start task, which is probably a DummyOperator. Then four parallel ingestion tasks. These could be PythonOperators that fetch data from the APIs. Since they're parallel, they should all depend on the start task and run simultaneously.

After ingestion, the next step is processing fetched metadata, which includes downloading CSVs. These tasks would depend on their respective ingestion tasks. For example, download nuclear CSV depends on fetching nuclear metadata. But wait, the description says "Process fetched metadata: Download nuclear CSV, thermal CSV, death files." So each download task depends on its corresponding fetch task. So maybe after the four parallel ingestion tasks, there are three download tasks (nuclear, thermal, death), each depending on their respective fetch task. The city coordinates might not need a download step since it's already a CSV fetched in the first step.

Then, create PostgreSQL tables. These tasks (deaths and power_plants tables) need to happen before data loading. So they should depend on the completion of the metadata processing (download tasks). But wait, the table creation is a separate step after metadata processing. So maybe the create_tables task group comes after all the download tasks are done.

Then parallel data processing: load death records into Redis, clean thermal data, clean nuclear data. These three tasks can run in parallel after the tables are created and the data is downloaded. So dependencies would be create_tables >> [load_death_records, clean_thermal, clean_nuclear].

Next, branching based on death data availability. Use BranchPythonOperator. The branch task checks if death data exists. If yes, proceed to cleanse and store death records; if not, skip to staging completion. The branch task depends on the load_death_records task. Because the load_death_records task is responsible for putting data into Redis, which might determine if data exists.

The death data processing path includes cleansing, storing in PostgreSQL, and cleaning temp files. These tasks depend on the branch choosing the 'yes' path. The other path skips to staging completion.

Power plant data processing involves generating SQL and storing in PostgreSQL. These tasks depend on the cleaning steps (clean_thermal and clean_nuclear) and the branch (since they might be in parallel? Or after the branch?). Wait, the pipeline steps say after the parallel data processing (load death, clean thermal, clean nuclear), then the branch occurs. Then, death processing path and power plant processing happen. So the power plant processing (generate SQL and store) should depend on the clean tasks and possibly happen regardless of the branch, but maybe after the branch? Or in parallel?

The description says "Power plant data processing" is a separate step after the branch. So maybe the power plant processing tasks depend on the clean tasks and the branch task. But since the branch is about death data, perhaps the power plant processing can proceed in parallel with the death data processing if the branch goes that way. Or maybe the power plant processing is part of the main flow and doesn't depend on the branch.

Wait, the pipeline steps list:

- Branch based on death data availability
  - If death data exists: proceed to cleanse and store death records
  - If no death data: skip to staging completion
- Death data processing path: ... 
- Power plant data processing: ... 
- Complete staging phase

So both death data processing and power plant processing need to complete before staging completion. So the power plant processing is not part of the branch; it's a separate path that happens regardless. So the generate_sql and store_power_plant tasks depend on the clean_thermal and clean_nuclear tasks, and possibly the branch task? Or can they run in parallel with the death processing?

Hmm. The description says "parallel data processing" includes loading death records, cleaning thermal, cleaning nuclear. Then branching occurs. Then death processing path and power plant processing. So perhaps the power plant processing is after the branching step. So the power plant tasks depend on the clean tasks and the branch task? Or maybe the branch and the power plant processing can be in parallel.

This is a bit ambiguous. Let me recheck the pipeline steps:

After the parallel data processing (load death, clean thermal, clean nuclear), the next step is branching based on death data. Then:

- If death data exists: cleanse, store, clean temp
- If not: skip to staging completion
- Power plant processing: generate SQL, store in PostgreSQL
- Complete staging phase

So the power plant processing is a separate step that happens after the parallel data processing and the branching. So the power plant tasks (generate SQL and store) should depend on the clean_thermal and clean_nuclear tasks, and also perhaps on the branching task? Or maybe they can run in parallel with the death processing if the branch allows.

Alternatively, maybe the power plant processing is part of the main flow and must happen regardless of the branch. So after the clean tasks, both the branch and the power plant processing can proceed.

Wait, the pipeline steps list "Power plant data processing" as a separate step after the branching. So perhaps the power plant processing is after the branching step. So the generate_sql and store_power_plant tasks depend on the clean tasks and the branch task. But since the branch is about death data, maybe the power plant processing can proceed regardless. So maybe the dependencies are:

clean_thermal >> generate_sql_thermal

clean_nuclear >> generate_sql_nuclear

But perhaps the power plant processing is a single task that combines both thermal and nuclear data. The description says "Generate SQL queries for plant data persistence" and "Store power plant records in PostgreSQL". So maybe there's a task to generate SQL for both thermal and nuclear, then store them.

Alternatively, maybe each clean task (thermal and nuclear) leads to their own generate and store tasks. But the description says "parallel data processing" includes cleaning thermal and nuclear, then power plant processing as a separate step. So perhaps after both clean tasks, generate SQL and store.

So the generate_sql task depends on both clean_thermal and clean_nuclear. Then store_power_plant depends on generate_sql.

Putting this together:

After the clean_thermal and clean_nuclear tasks, generate_sql runs, then store_power_plant.

So dependencies would be:

[clean_thermal, clean_nuclear] >> generate_sql >> store_power_plant

Then, the staging completion task depends on both the power plant processing and the death data processing (if applicable) or the branch's 'skip' path.

So the complete staging phase task (maybe a DummyOperator) is the final step, which depends on store_power_plant and either the death_store task or the branch's 'skip' path.

Now, task groups: the user mentioned using task groups for logical grouping of ingestion and staging phases. So ingestion phase includes the four fetch tasks and the three download tasks. Then staging phase includes create tables, data processing, branching, etc.

Wait, the pipeline steps have:

- Start
- Parallel ingestion (4 sources)
- Process fetched metadata (download CSV/data)
- Create tables
- Parallel data processing (load death, clean thermal, clean nuclear)
- Branch
- Death processing path
- Power plant processing
- Complete staging

So perhaps the ingestion phase is the first two steps (fetch and download), then staging includes the rest. Or maybe the task groups are "ingestion" and "staging". Let's see.

The user's example says "Includes task groups for logical grouping of ingestion and staging phases". So:

- ingestion_task_group: includes the four fetch tasks and the three download tasks.
- staging_task_group: includes create tables, data processing, branching, etc.

But how to structure that. In Airflow, task groups are used to group tasks in the UI. So the fetch tasks and download tasks would be inside the ingestion group. Then the create tables, data processing tasks, etc., in the staging group.

Now, imports: need to use Airflow 2.x style. So from airflow import DAG, from airflow.operators.python import PythonOperator, etc. Also, if sensors are used, but in the description, there's no mention of sensors. Wait, the pipeline steps don't mention waiting for files or external tasks, so maybe no sensors here. The user mentioned that if sensors appear, demonstrate proper use, but in this case, the steps involve fetching data from APIs, which would likely use PythonOperator to make HTTP requests, not sensors.

Now, the branching: use BranchPythonOperator. The branch task checks if death data exists. The function for the branch operator would check if the data is available (maybe by checking Redis or some other storage). Since this is a example, perhaps the function returns the task ID of either the cleanse_death_records task or the skip_staging task.

The death data processing path includes cleanse, store, clean_temp. These tasks are sequential: cleanse >> store >> clean_temp.

The power plant processing is generate_sql >> store_power_plant.

The staging completion task is the final step, which depends on both the power plant processing and the death processing (if it happened) or the skip path.

Now, default_args should include retries, retry_delay, maybe other parameters like email on failure, but the user didn't specify, so just include what's mentioned: retry mechanism with 10-second delay. So default_args = {'retries': 1, 'retry_delay': timedelta(seconds=10)}.

The DAG parameters: schedule_interval=None, catchup=False, default_args, max_active_runs=1 (concurrency).

Now, putting all together:

Start with imports:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.branch import BranchPythonOperator

Then default_args:

default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    # Other parameters if needed, but user didn't specify
}

Then the DAG definition:

with DAG(
    dag_id='french_data_etl',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'french_data'],
) as dag:

Then the tasks. Start with start_pipeline = DummyOperator(task_id='start_pipeline')

Then the ingestion_task_group:

with TaskGroup('ingestion') as ingestion_tg:
    # Four fetch tasks
    fetch_nuclear_meta = PythonOperator(
        task_id='fetch_nuclear_metadata',
        python_callable=lambda: print("Fetching nuclear metadata"),
    )
    fetch_thermal_meta = PythonOperator(
        task_id='fetch_thermal_metadata',
        python_callable=lambda: print("Fetching thermal metadata"),
    )
    fetch_death_resources = PythonOperator(
        task_id='fetch_death_resources',
        python_callable=lambda: print("Fetching death records resource list"),
    )
    fetch_city_coords = PythonOperator(
        task_id='fetch_city_coordinates',
        python_callable=lambda: print("Fetching city coordinates CSV"),
    )

    # Three download tasks
    download_nuclear = PythonOperator(
        task_id='download_nuclear_data',
        python_callable=lambda: print("Downloading nuclear CSV"),
    )
    download_thermal = PythonOperator(
        task_id='download_thermal_data',
        python_callable=lambda: print("Downloading thermal CSV"),
    )
    download_death_files = PythonOperator(
        task_id='download_death_files',
        python_callable=lambda: print("Downloading death record files"),
    )

    # Dependencies: each download depends on its fetch
    fetch_nuclear_meta >> download_nuclear
    fetch_thermal_meta >> download_thermal
    fetch_death_resources >> download_death_files
    # fetch_city_coords has no download task? Because it's already CSV?

    # The four fetch tasks are parallel, so start_pipeline >> [all four fetch tasks]
    # But within the task group, how to set dependencies? The start_pipeline is outside the task group.

Wait, the start_pipeline is a DummyOperator outside the task group. So the ingestion_tg's tasks should depend on start_pipeline. So:

start_pipeline >> [fetch_nuclear_meta, fetch_thermal_meta, fetch_death_resources, fetch_city_coords]

Then, after the ingestion_task_group, the download tasks are part of the ingestion group. Then the next step is the create_tables task group.

Wait, the create_tables comes after the download tasks. So after the ingestion_tg, the create_tables group runs.

So:

start_pipeline >> ingestion_tg

Then, after ingestion_tg, create_tables.

But the create_tables depends on the download tasks. Wait, no. The create_tables is a separate task group. Wait, the pipeline steps after ingestion and processing metadata (downloads) is create tables. So the create_tables tasks must depend on the download tasks completing.

But within the ingestion_tg, the download tasks are part of the group. So after the ingestion_tg completes, the create_tables group starts.

So:

ingestion_tg >> create_tables_group

But how to structure the create_tables group.

Wait, perhaps:

with TaskGroup('create_tables') as create_tables_tg:
    create_deaths_table = PythonOperator(
        task_id='create_deaths_table',
        python_callable=lambda: print("Creating deaths table in PostgreSQL"),
    )
    create_power_plants_table = PythonOperator(
        task_id='create_power_plants_table',
        python_callable=lambda: print("Creating power_plants table in PostgreSQL"),
    )

    # These can be parallel
    create_deaths_table
    create_power_plants_table

But the create tables tasks don't depend on each other. So they can run in parallel.

Then, after create_tables_tg, the data processing tasks (load_death, clean_thermal, clean_nuclear) run in parallel.

So:

create_tables_tg >> [load_death_records, clean_thermal, clean_nuclear]

Wait, but load_death_records, clean_thermal, clean_nuclear are part of the staging phase. So perhaps another task group for staging.

But maybe the task groups are ingestion and staging. So:

After the ingestion_tg, the create_tables_tg is part of staging_tg.

Alternatively, the staging_tg includes create_tables, data processing, branching, etc.

But perhaps the user wants ingestion and staging as separate task groups.

So:

with TaskGroup('staging') as staging_tg:
    create_deaths_table = ...
    create_power_plants_table = ...
    # Then data processing tasks

But this might complicate dependencies. Alternatively, structure the DAG as:

start_pipeline >> ingestion_tg >> create_tables >> data_processing_tasks >> ...

But perhaps better to have the create_tables as part of the staging phase.

Alternatively, the task groups are 'ingestion' and 'staging', with staging including create tables, data processing, etc.

So:

After the ingestion_tg, the staging_tg begins.

Within staging_tg:

- create tables
- data processing (load, clean)
- branching
- death processing
- power plant processing
- staging completion

So:

with TaskGroup('staging') as staging_tg:
    create_deaths_table = ...
    create_power_plants_table = ...

    # Then data processing tasks
    load_death_records = PythonOperator(...)
    clean_thermal = PythonOperator(...)
    clean_nuclear = PythonOperator(...)

    # Branching
    check_death_data = BranchPythonOperator(...)

    # Death processing path
    cleanse_death = PythonOperator(...)
    store_death = PythonOperator(...)
    clean_temp = PythonOperator(...)

    # Power plant processing
    generate_sql = PythonOperator(...)
    store_power_plant = PythonOperator(...)

    # Staging completion
    complete_staging = DummyOperator(...)

    # Dependencies within staging_tg
    [create_deaths_table, create_power_plants_table] >> [load_death_records, clean_thermal, clean_nuclear]
    load_death_records >> check_death_data
    check_death_data >> [cleanse_death, skip_staging]
    cleanse_death >> store_death >> clean_temp
    [clean_thermal, clean_nuclear] >> generate_sql
    generate_sql >> store_power_plant
    [clean_temp, store_power_plant, skip_staging] >> complete_staging

Wait, but the skip_staging would be a task that skips to complete_staging. So:

skip_staging = DummyOperator(task_id='skip_staging', trigger_rule='none_failed')

But the BranchPythonOperator returns either 'cleanse_death_records' or 'skip_staging'.

So:

def check_death_data_func(**kwargs):
    # Assume some logic to check if data exists; for example, check a file or Redis
    # In this example, just assume data exists for demonstration
    data_exists = True  # This should be replaced with actual check
    if data_exists:
        return 'cleanse_death_records'
    else:
        return 'skip_staging'

check_death_data = BranchPythonOperator(
    task_id='check_death_data_availability',
    python_callable=check_death_data_func,
)

Then, the death processing path:

cleanse_death_records = PythonOperator(...)
store_death_records = PythonOperator(...)
clean_temp_files = PythonOperator(...)

cleanse_death_records >> store_death_records >> clean_temp_files

The skip_staging is a DummyOperator.

Then, the complete_staging task depends on clean_temp_files (if data existed) or skip_staging, and also on store_power_plant.

So:

clean_temp_files >> complete_staging
skip_staging >> complete_staging
store_power_plant >> complete_staging

But how to set these dependencies. The complete_staging should be triggered when either the death processing is done (including clean_temp) and the power plant processing is done, or if death data was skipped and power plant is done.

So:

[clean_temp_files, store_power_plant, skip_staging] >> complete_staging

Wait, no. Because store_power_plant and skip_staging are separate. Wait, the power plant processing (generate_sql and store_power_plant) must complete regardless of the branch. So store_power_plant must always run, and the complete_staging depends on both store_power_plant and either clean_temp_files or skip_staging.

So:

store_power_plant >> complete_staging
clean_temp_files >> complete_staging
skip_staging >> complete_staging

But that would require complete_staging to wait for all three, which is not correct. Because if the branch skips, then skip_staging and store_power_plant must both complete. If the branch proceeds, then clean_temp_files and store_power_plant must complete.

So the correct dependencies are:

(clean_temp_files or skip_staging) and store_power_plant must complete before complete_staging.

In Airflow, you can use a list with the >> operator. So:

[store_power_plant, clean_temp_files, skip_staging] >> complete_staging

But that would require all three to complete, which isn't correct. Because in the case where death data exists, skip_staging is not executed. So the trigger rule for complete_staging should be TriggerRule.ALL_DONE or something else. Wait, no. The complete_staging should run when either (store_power_plant and clean_temp_files) or (store_power_plant and skip_staging) are done.

But how to model this. Since store_power_plant is always required, and either clean_temp_files or skip_staging is required.

So:

store_power_plant must run, and either clean_temp_files or skip_staging must run.

So dependencies:

store_power_plant >> complete_staging
clean_temp_files >> complete_staging
skip_staging >> complete_staging

But complete_staging will trigger when all upstream tasks are done. But in reality, only store_power_plant and one of clean_temp_files or skip_staging will run. So using trigger_rule='all_done' might work, but perhaps better to have complete_staging depend on store_power_plant and the branch's outcome.

Alternatively, use a join task that depends on store_power_plant and the death processing path.

But perhaps the simplest way is to have complete_staging depend on both store_power_plant and the branch's outcome (either clean_temp_files or skip_staging). So:

store_power_plant >> complete_staging
clean_temp_files >> complete_staging
skip_staging >> complete_staging

But in Airflow, if a task is skipped (like skip_staging when data exists), then the complete_staging would still run because one of the upstream tasks (clean_temp_files) succeeded, and the other (skip_staging) is skipped. But the trigger rule for complete_staging should be 'none_failed' or 'all_done'.

Wait, the default trigger rule is 'all_success', which would require all upstream tasks to have succeeded. But in the case where skip_staging is skipped, then 'all_success' would still allow complete_staging to run if the other upstream tasks succeeded.

Wait, no. If a task is skipped, then in Airflow, the trigger rule 'all_success' requires all upstream tasks to have succeeded or been skipped. Wait, no. The trigger rule 'all_success' means all upstream tasks must have succeeded. If any upstream task is skipped, then the task with 'all_success' trigger rule will be skipped.

Wait, no. Let me check: the trigger rule determines when a task is triggered based on the state of its upstream tasks. The default is 'all_success', meaning all upstream tasks must have succeeded for the task to be triggered.

If one upstream task is skipped, then the task with 'all_success' trigger rule will not be triggered. So in this case, complete_staging has three upstream tasks: store_power_plant, clean_temp_files, skip_staging. Depending on the branch, either clean_temp_files and store_power_plant are successful, or skip_staging and store_power_plant are successful.

But with 'all_success' trigger rule, complete_staging would require all three upstream tasks to be successful, which is not the case. So we need to set the trigger_rule to 'none_failed'. Or perhaps 'one_success'.

Alternatively, structure the dependencies such that complete_staging depends on store_power_plant and either clean_temp_files or skip_staging.

This can be achieved by making complete_staging depend on store_power_plant and a dummy task that merges the death processing path.

Alternatively, use the TriggerRule.ALL_DONE, which means the task runs as long as all upstream tasks are done, regardless of their state. But that's not ideal because if any upstream task failed, complete_staging would still run. But according to the pipeline description, retries are in place, so perhaps failures are handled.

Alternatively, use TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS. But that's more complex.

Alternatively, structure the dependencies so that complete_staging depends on store_power_plant and a task that is triggered by either clean_temp_files or skip_staging.

For example:

After the death processing path (clean_temp_files) and the skip_staging, create a dummy task that merges these two paths, then have complete_staging depend on both the merge task and store_power_plant.

So:

merge_death_path = DummyOperator(task_id='merge_death_path', trigger_rule='none_failed')

clean_temp_files >> merge_death_path
skip_staging >> merge_death_path

Then:

merge_death_path >> complete_staging
store_power_plant >> complete_staging

This way, complete_staging depends on both merge_death_path and store_power_plant. The merge_death_path is triggered if either clean_temp_files or skip_staging completes.

This ensures that complete_staging runs only when both store_power_plant and either death processing or skip are done.

So integrating this into the code:

Within the staging task group:

merge_death_path = DummyOperator(
    task_id='merge_death_path',
    trigger_rule='none_failed',
)

clean_temp_files >> merge_death_path
skip_staging >> merge_death_path

merge_death_path >> complete_staging
store_power_plant >> complete_staging

Now, complete_staging depends on both merge_death_path and store_power_plant.

Putting all together:

The code would look like this:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.branch import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='french_data_etl',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'french_data'],
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup('ingestion') as ingestion_tg:
        fetch_nuclear_meta = PythonOperator(
            task_id='fetch_nuclear_metadata',
            python_callable=lambda: print("Fetching nuclear metadata"),
        )
        fetch_thermal_meta = PythonOperator(
            task_id='fetch_thermal_metadata',
            python_callable=lambda: print("Fetching thermal metadata"),
        )
        fetch_death_resources = PythonOperator(
            task_id='fetch_death_resources',
            python_callable=lambda: print("Fetching death records resource list"),
        )
        fetch_city_coords = PythonOperator(
            task_id='fetch_city_coordinates',
            python_callable=lambda: print("Fetching city coordinates CSV"),
        )

        download_nuclear = PythonOperator(
            task_id='download_nuclear_data',
            python_callable=lambda: print("Downloading nuclear CSV"),
        )
        download_thermal = PythonOperator(
            task_id='download_thermal_data',
            python_callable=lambda: print("Downloading thermal CSV"),
        )
        download_death_files = PythonOperator(
            task_id='download_death_files',
            python_callable=lambda: print("Downloading death record files"),
        )

        # Set dependencies within ingestion group
        fetch_nuclear_meta >> download_nuclear
        fetch_thermal_meta >> download_thermal
        fetch_death_resources >> download_death_files
        # fetch_city_coords has no download task

    with TaskGroup('staging') as staging_tg:
        create_deaths_table = PythonOperator(
            task_id='create_deaths_table',
            python_callable=lambda: print("Creating deaths table in PostgreSQL"),
        )
        create_power_plants_table = PythonOperator(
            task_id='create_power_plants_table',
            python_callable=lambda: print("Creating power_plants table in PostgreSQL"),
        )

        load_death_records = PythonOperator(
            task_id='load_death_records_to_redis',
            python_callable=lambda: print("Loading death records into Redis"),
        )
        clean_thermal = PythonOperator(
            task_id='clean_thermal_plant_data',
            python_callable=lambda: print("Cleaning thermal plant data"),
        )
        clean_nuclear = PythonOperator(
            task_id='clean_nuclear_plant_data',
            python_callable=lambda: print("Cleaning nuclear plant data"),
        )

        # Branching
        def check_death_data(**kwargs):
            # Example check: assume data exists
            data_exists = True  # Replace with actual check
            if data_exists:
                return 'staging.cleanse_death_records'
            else:
                return 'staging.skip_staging'

        check_death_data_op = BranchPythonOperator(
            task_id='check_death_data_availability',
            python_callable=check_death_data,
        )

        # Death data processing path
        cleanse_death_records = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=lambda: print("Cleansing death records"),
        )
        store_death_records = PythonOperator(
            task_id='store_death_records',
            python_callable=lambda: print("Storing death records in PostgreSQL"),
        )
        clean_temp_files = PythonOperator(
            task_id='clean_temp_death_files',
            python_callable=lambda: print("Cleaning temporary death data files"),
        )

        # Skip path
        skip_staging = DummyOperator(
            task_id='skip_staging',
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        # Power plant processing
        generate_sql = PythonOperator(
            task_id='generate_sql_queries',
            python_callable=lambda: print("Generating SQL for plant data"),
        )
        store_power_plant = PythonOperator(
            task_id='store_power_plant_data',
            python_callable=lambda: print("Storing power plant records in PostgreSQL"),
        )

        # Merge death path
        merge_death_path = DummyOperator(
            task_id='merge_death_path',
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        # Staging completion
        complete_staging = DummyOperator(
            task_id='complete_staging_phase',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        # Dependencies within staging group
        [create_deaths_table, create_power_plants_table] >> [load_death_records, clean_thermal, clean_nuclear]
        load_death_records >> check_death_data_op
        check_death_data_op >> [cleanse_death_records, skip_staging]
        cleanse_death_records >> store_death_records >> clean_temp_files
        [clean_thermal, clean_nuclear] >> generate_sql
        generate_sql >> store_power_plant
        clean_temp_files >> merge_death_path
        skip_staging >> merge_death_path
        [merge_death_path, store_power_plant] >> complete_staging

    # Set dependencies between task groups
    start_pipeline >> ingestion_tg
    ingestion_tg >> [staging_tg.create_deaths_table, staging_tg.create_power_plants_table]

Wait, no. The create_deaths_table and create_power_plants_table are part of the staging_tg. So after the ingestion_tg completes, the create tables tasks must run. But how to set dependencies between ingestion_tg and staging_tg's create tables.

The ingestion_tg's download tasks (download_nuclear, download_thermal, download_death_files) must complete before the create tables tasks. So:

ingestion_tg >> staging_tg.create_deaths_table
ingestion_tg >> staging_tg.create_power_plants_table

But since the create tables are part of the staging_tg, perhaps the entire ingestion_tg should precede the staging_tg. So:

start_pipeline >> ingestion_tg >> staging_tg

But within the staging_tg, the create tables tasks depend on the ingestion_tg's tasks. So:

In the DAG, after defining ingestion_tg and staging_tg:

start_pipeline >> ingestion_tg
ingestion_tg >> staging_tg

But this would mean that the entire staging_tg depends on the entire ingestion_tg. But within the staging_tg, the create tables tasks are the first to run. So perhaps that's acceptable.

Alternatively, the create tables tasks depend on the download tasks. For example, create_deaths_table depends on download_death_files, and create_power_plants_table depends on download_nuclear and download_thermal.

But since the download tasks are part of the ingestion_tg, and the create tables are part of staging_tg, how to reference them.

In Airflow, when using task groups, the task IDs are prefixed with the group ID. For example, the download_death_files task in ingestion_tg has task_id 'ingestion.download_death_files'.

So, to set dependencies between tasks in different groups:

download_death_files = ingestion_tg.download_death_files
create_deaths_table = staging_tg.create_deaths_table

download_death_files >> create_deaths_table

But this would require that create_deaths_table depends on download_death_files.

Similarly, create_power_plants_table depends on download_nuclear and download_thermal.

But according to the pipeline steps, the create tables step comes after processing fetched metadata (downloads). So each create table task depends on the relevant download tasks.

For example:

create_deaths_table depends on download_death_files.

create_power_plants_table depends on download_nuclear and download_thermal.

But how to model this.

Alternatively, perhaps the create tables can be dependent on all download tasks. But according to the pipeline steps, the create tables are created after all downloads, but maybe each table depends on specific downloads.

But for simplicity, perhaps the create tables tasks depend on all download tasks.

But in the code above, the create_deaths_table and create_power_plants_table are part of the staging_tg, and the download tasks are part of the ingestion_tg.

So dependencies would be:

ingestion_tg.download_death_files >> staging_tg.create_deaths_table
ingestion_tg.download_nuclear >> staging_tg.create_power_plants_table
ingestion_tg.download_thermal >> staging_tg.create_power_plants_table

But how to reference these tasks.

In Airflow, when using task groups, you can access tasks using the group's name. For example, if the ingestion_tg is named 'ingestion', then the download_death_files task's ID is 'ingestion.download_death_files'.

But when defining dependencies between task groups, you can use the syntax:

ingestion_tg.get_task('download_death_files') >> staging_tg.get_task('create_deaths_table')

But in practice, when using the TaskGroup context manager, the tasks are added to the DAG, and you can reference them using their task_id with the group's prefix.

Alternatively, within the DAG context, after defining the ingestion_tg and staging_tg, set the dependencies:

download_death_files = ingestion_tg.download_death_files
create_deaths_table = staging_tg.create_deaths_table
download_death_files >> create_deaths_table

But this may not work because the tasks are part of the task groups. Alternatively, use the task IDs with the group prefixes.

But this can get complicated. Perhaps a better approach is to structure the DAG such that the create tables tasks depend on the download tasks from the ingestion group.

So, after defining both task groups, set dependencies:

ingestion_tg.download_death_files >> staging_tg.create_deaths_table
ingestion_tg.download_nuclear >> staging_tg.create_power_plants_table
ingestion_tg.download_thermal >> staging_tg.create_power_plants_table

But in code, how to reference these tasks. For example, if the ingestion_tg is a TaskGroup, then the tasks inside can be accessed via their task_id with the group's ID as a prefix.

But in the code above, the tasks are defined within the TaskGroup context, so their task_id includes the group's name. For example, the download_death_files task in the 'ingestion' group has task_id 'ingestion.download_death_files'.

So, to set the dependency:

download_death_files_task = dag.get_task('ingestion.download_death_files')
create_deaths_table_task = dag.get_task('staging.create_deaths_table')
download_death_files_task >> create_deaths_table_task

But this is not straightforward to write in the code, especially when the tasks are defined within the TaskGroup context managers.

An alternative approach is to avoid using task groups for create_tables and staging, and instead structure the DAG with explicit dependencies.

But the user specified to use task groups for logical grouping of ingestion and staging phases. So perhaps the ingestion_tg includes the fetch and download tasks, and the staging_tg includes create tables, data processing, branching, etc.

But the dependencies between the