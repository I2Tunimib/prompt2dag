<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline involves several steps: data ingestion from four sources in parallel, processing the metadata, creating PostgreSQL tables, parallel data processing, conditional branching, and then different paths based on data availability. The DAG should use Airflow 2.x style imports, proper task dependencies, and include retries with a 10-second delay.

The user mentioned manual triggers only, so schedule_interval is None. Also, maximum concurrency is 1, so I'll set concurrency in default_args. Catchup should be False to prevent backfilling.

Starting with the imports. They specified using PythonOperator, and since there are sensors mentioned, but in the pipeline steps I don't see any sensors. Wait, the pipeline steps mention conditional branching based on data availability. So maybe a BranchPythonOperator is needed there.

The first step is the start pipeline execution, which could be a dummy operator. Then four parallel ingestion tasks. These would be PythonOperators that fetch data from the APIs. Then processing the fetched metadata, which involves downloading CSV data and death records. These tasks should depend on the respective ingestion tasks.

Next, creating PostgreSQL tables. These tasks need to run after the metadata processing. Then parallel data processing tasks: loading death records into Redis, cleaning thermal and nuclear data. These depend on the previous steps.

Branching based on death data availability. Use BranchPythonOperator here. The branch function checks if death data exists. If yes, proceed with cleansing, storing, and cleaning. If not, skip to staging completion.

The death data path has several steps, and the power plant processing happens regardless. Finally, complete staging phase.

I need to structure the dependencies correctly. The initial parallel ingestion (four tasks) should all run first. Then the metadata processing tasks (download CSV, etc.) depend on their respective ingestion tasks. For example, download nuclear CSV depends on fetch nuclear metadata.

Then, the create tables tasks (deaths and power_plants) must come after the metadata processing. Wait, the create tables are separate steps. Maybe they should come after the metadata processing but before the data processing steps.

Wait, the pipeline steps say:

- Process fetched metadata (download CSV data, etc.)
- Create PostgreSQL tables
- Parallel data processing (load death records, clean plant data)

So the create tables must happen after processing metadata but before data processing. So dependencies would be: after process metadata tasks, create tables, then data processing.

Then the data processing tasks (load death, clean thermal, clean nuclear) can run in parallel. After that, the branching occurs.

The branch checks if death data exists. If yes, do the death processing path; if not, skip to staging completion. The power plant processing (generate SQL and store) should happen regardless, but maybe after the branch? Or in parallel?

The pipeline steps mention that after the branching, the death data processing path includes cleansing, storing, and cleaning. The power plant data processing (generate SQL and store) is a separate step. Then complete staging phase.

So dependencies:

After the parallel data processing tasks (load death, clean thermal, clean nuclear), the branch occurs. Then, depending on the branch, either the death data steps proceed or skip. The power plant processing can happen in parallel with the death data steps, or after? The description says "Power plant data processing" is a separate step, so maybe it's after the branch. But the pipeline steps list it as a separate bullet after the branch, so perhaps it's part of the main flow.

Wait, the steps are:

- Branch based on death data availability
- Death data processing path (if data exists)
- Power plant data processing
- Complete staging phase

So the power plant processing happens after the branch, regardless of the death data path. So the power plant processing depends on the data processing tasks (load death, clean thermal, clean nuclear), and the branch task.

Wait, maybe the power plant processing can run in parallel with the death data processing if the branch proceeds. But according to the description, the steps are sequential: after branching, death data processing, then power plant processing, then staging completion. Or maybe the power plant processing is part of the main flow and not dependent on the branch.

Hmm, the pipeline steps list "Power plant data processing" as a separate step after the branch. So perhaps the power plant processing happens after the branch task, but not part of either branch. Or maybe it's part of the main flow that happens regardless of the branch.

Wait, looking at the pipeline steps:

After the parallel data processing (load death, clean thermal, clean nuclear), the branch occurs. Then:

- If death data exists: proceed to cleanse, store, clean
- If not: skip to staging completion

Then, power plant data processing (generate SQL, store in PostgreSQL)

Then complete staging phase.

So the power plant processing happens after the branch, but not part of either branch. So regardless of the death data path, power plant processing must occur. Then staging completion.

Wait, but the steps list "power plant data processing" as a separate bullet after the branch. So perhaps the power plant processing is part of the main flow and happens after the branch task. So dependencies would be:

branch_task >> [death_cleanse_task, staging_completion_task]

Then power_plant_processing_task depends on branch_task, but how? Because regardless of the branch outcome, power plant processing must happen.

Wait, maybe the branch only affects the death data path, but the power plant processing is independent and must happen after the data processing steps. Let me re-examine the pipeline steps:

The steps after the branch are:

- Death data processing path (if data exists)
- Power plant data processing
- Complete staging phase

So regardless of the death data path, power plant processing happens. So the power plant processing task should depend on the data processing tasks (load death, clean thermal, clean nuclear) and the branch task. But how to structure that.

Alternatively, maybe the power plant processing is part of the main flow and happens after the data processing tasks, and the branch is only for the death data path.

So the dependencies would be:

data_processing_tasks (load_death, clean_thermal, clean_nuclear) >> branch_task

branch_task >> [death_cleanse_task, staging_completion_task]

Then power_plant_processing_task must run after data_processing_tasks and perhaps after the branch_task. But according to the pipeline steps, power plant processing is after the branch. So perhaps:

data_processing_tasks >> branch_task

branch_task >> death_cleanse_task >> store_death_task >> clean_temp_files_task

branch_task >> staging_completion_task

Then power_plant_processing_task must run after data_processing_tasks, but not necessarily after the branch_task. Wait, no. Because the pipeline steps list power plant processing as a step after the branch. So maybe power_plant_processing_task depends on the data_processing_tasks and the branch_task.

But how to model that. Let's think:

The data_processing_tasks (load_death, clean_thermal, clean_nuclear) all run first. Then branch_task runs. Then, regardless of the branch outcome, power_plant_processing_task should run. So power_plant_processing_task depends on branch_task. But also, perhaps power_plant_processing_task can run in parallel with the death data processing tasks if the branch proceeds.

Wait, the pipeline steps say:

After branching:

- If death data exists: proceed to death processing steps
- Else: skip to staging completion

Then power plant processing happens, then staging completion.

So power plant processing must happen after the branch, but not part of either branch. So regardless of the death data path, power plant processing must occur. Then staging completion happens after both power plant processing and the death data path (if taken) or staging completion (if skipped).

Wait, the steps are:

After branching:

Death data path (if taken):

- Cleanse death records
- Store in PostgreSQL
- Clean temp files

Then power plant processing:

- Generate SQL
- Store in PostgreSQL

Then complete staging phase.

If death data not taken:

Skip to staging completion, but power plant processing still needs to happen.

Wait, this is a bit ambiguous. The pipeline steps list "power plant data processing" as a separate step after the branch. So perhaps the power plant processing is part of the main flow and must happen regardless of the branch outcome. So the dependencies would be:

After branch_task, both death path (if taken) and power plant processing happen, then staging completion depends on both.

But how to model that. Let's outline the tasks:

branch_task >> death_cleanse_task >> store_death_task >> clean_temp_files_task

branch_task >> staging_completion_skip

power_plant_processing_task depends on data_processing_tasks and branch_task?

Wait, maybe the power_plant_processing_task is part of the main flow and must run after the data_processing_tasks. But according to the pipeline steps, the power plant processing is after the branch. So perhaps:

data_processing_tasks >> branch_task

branch_task >> [death_cleanse_task, staging_completion_skip]

Then power_plant_processing_task must run after branch_task, but not part of either branch. So:

branch_task >> power_plant_processing_task

power_plant_processing_task >> staging_completion

But then staging_completion would depend on power_plant_processing_task and either clean_temp_files_task or staging_completion_skip.

Wait, this is getting complicated. Let me try to map the steps:

1. Start
2. Parallel ingestion (4 tasks)
3. Process metadata (download CSV, etc.) - these depend on respective ingestion tasks
4. Create tables (depends on process metadata)
5. Parallel data processing (load death, clean thermal, clean nuclear) - depend on create tables
6. Branch based on death data
   - If yes: cleanse, store, clean temp
   - If no: skip to staging completion
7. Power plant processing (generate SQL, store)
8. Complete staging phase (depends on power plant processing and death path if taken)

Wait, but according to the pipeline steps, after the branch, the power plant processing is a separate step. So perhaps:

After the branch, regardless of the outcome, power plant processing must happen. Then staging completion happens after both the power plant processing and the death path (if taken) or staging completion skip.

So the dependencies would be:

branch_task >> [death_cleanse_task, staging_completion_skip]

death_cleanse_task >> store_death_task >> clean_temp_files_task

Then power_plant_processing_task must run after branch_task. So:

branch_task >> power_plant_processing_task

power_plant_processing_task >> staging_completion

But also, staging_completion must wait for clean_temp_files_task (if death data exists) or staging_completion_skip (if not). So:

[clean_temp_files_task, staging_completion_skip, power_plant_processing_task] >> staging_completion

Wait, but staging_completion_skip is a task that's triggered when there's no death data. So staging_completion_skip would be a dummy task that's triggered by the branch, and staging_completion depends on both power_plant_processing_task and either clean_temp_files_task or staging_completion_skip.

So:

branch_task >> staging_completion_skip

Then:

[clean_temp_files_task, staging_completion_skip, power_plant_processing_task] >> staging_completion

But that's not possible because clean_temp_files_task is part of the death data path, which only runs if the branch selects it. Similarly, staging_completion_skip is part of the no-death-data path.

So perhaps:

staging_completion depends on power_plant_processing_task and either clean_temp_files_task or staging_completion_skip.

But in Airflow, you can't have a task that depends on one of two paths. You need to have all upstream tasks connected. So maybe:

power_plant_processing_task >> staging_completion

clean_temp_files_task >> staging_completion

staging_completion_skip >> staging_completion

But that would require all three to run, which isn't correct. Because in reality, either clean_temp_files_task or staging_completion_skip runs, but not both.

So perhaps:

staging_completion is triggered after power_plant_processing_task and whichever of clean_temp_files_task or staging_completion_skip ran.

But how to model that. Maybe use a dummy task after clean_temp_files_task and staging_completion_skip, then have staging_completion depend on that dummy task and power_plant_processing_task.

Alternatively, use a join task that depends on both paths.

Wait, maybe the staging_completion task depends on power_plant_processing_task and the death data path (if taken) or staging_completion_skip (if not). But Airflow can't dynamically change dependencies based on task outcomes. So the solution is to have staging_completion depend on power_plant_processing_task and both possible paths (clean_temp_files_task and staging_completion_skip), but set trigger_rule to 'none_failed' or 'all_done'.

Alternatively, set trigger_rule to 'none_failed' for staging_completion, so it runs as long as all upstream tasks have completed, regardless of their status. But that's not ideal.

Alternatively, use a trigger rule of 'none_failed_min_one_success' or similar. Wait, Airflow's trigger rules include 'one_success', 'all_done', etc. Maybe set staging_completion's trigger_rule to 'all_done', so it runs regardless of whether the upstream tasks succeeded or failed. But that might not be desired.

Alternatively, create a dummy task that is downstream of both clean_temp_files_task and staging_completion_skip, then have staging_completion depend on that dummy task and power_plant_processing_task.

For example:

branch_task >> death_cleanse_task >> store_death_task >> clean_temp_files_task

branch_task >> staging_completion_skip

Then create a task group or a dummy task that depends on both clean_temp_files_task and staging_completion_skip. Let's call it death_data_final:

clean_temp_files_task >> death_data_final

staging_completion_skip >> death_data_final

Then death_data_final and power_plant_processing_task both must complete before staging_completion.

So:

death_data_final >> staging_completion

power_plant_processing_task >> staging_completion

This way, regardless of which path was taken (death data or not), death_data_final runs after either clean_temp_files_task or staging_completion_skip. Then staging_completion depends on both death_data_final and power_plant_processing_task.

But how to implement death_data_final. Since it's a dummy task that depends on both paths, we can use a DummyOperator.

So:

from airflow.operators.dummy import DummyOperator

death_data_final = DummyOperator(task_id='death_data_final')

clean_temp_files_task >> death_data_final

staging_completion_skip >> death_data_final

Then:

death_data_final >> staging_completion

power_plant_processing_task >> staging_completion

This ensures that staging_completion runs after both power_plant_processing and whichever death data path was taken.

Now, putting all together.

The tasks are:

start_pipeline = DummyOperator(...)

ingestion_tasks: fetch_nuclear_meta, fetch_thermal_meta, fetch_death_resources, fetch_city_coords

process_metadata: download_nuclear_csv, download_thermal_csv, download_death_files

create_tables: create_deaths_table, create_power_plants_table

data_processing: load_death_records, clean_thermal_data, clean_nuclear_data

branch_task = BranchPythonOperator(...)

death_cleanse_task, store_death_task, clean_temp_files_task

staging_completion_skip = DummyOperator(...)

power_plant_processing: generate_sql_queries, store_power_plants

death_data_final = DummyOperator(...)

staging_completion = DummyOperator(...)

Now dependencies:

start_pipeline >> [fetch_nuclear_meta, fetch_thermal_meta, fetch_death_resources, fetch_city_coords]

fetch_nuclear_meta >> download_nuclear_csv

fetch_thermal_meta >> download_thermal_csv

fetch_death_resources >> download_death_files

fetch_city_coords >> ...? Wait, the city coordinates are part of the metadata processing? Or is the download_death_files part of the metadata processing?

Wait, the pipeline steps:

Process fetched metadata:

- Download nuclear CSV

- Download thermal CSV

- Download death record files

So each of these depends on their respective ingestion tasks. So:

fetch_nuclear_meta >> download_nuclear_csv

fetch_thermal_meta >> download_thermal_csv

fetch_death_resources >> download_death_files

fetch_city_coords: the city coordinates CSV is fetched, but where is it processed? The pipeline steps mention that city geographic coordinates CSV is fetched during ingestion, but the processing steps don't mention it. Maybe it's used in the death data processing for geographic enrichment. But in the DAG, perhaps the download of city coordinates is part of the metadata processing. Wait, the pipeline steps under "Process fetched metadata" don't mention downloading city coordinates CSV. So perhaps the city coordinates are fetched but not processed until later steps. Hmm, maybe the city coordinates are used in the death data cleansing step (geographic enrichment). So the download_death_files task may depend on fetch_city_coords? Or perhaps the city coordinates are part of the metadata processing.

This part is a bit unclear. The pipeline description says:

Parallel data ingestion from four sources includes fetching city geographic coordinates CSV. Then process fetched metadata includes downloading death record files. So perhaps the city coordinates CSV is downloaded during the metadata processing step. But the description says "Process fetched metadata: download nuclear CSV, thermal CSV, death files". So maybe the city coordinates are fetched but not processed until later.

In any case, according to the pipeline steps, the four ingestion tasks are:

- Fetch nuclear metadata

- Fetch thermal metadata

- Fetch death records resource list

- Fetch city coordinates CSV

Then process fetched metadata:

- Download nuclear CSV

- Download thermal CSV

- Download death files

So the download_death_files task depends on fetch_death_resources. The city coordinates CSV is fetched but not processed in the metadata step. So perhaps the city coordinates CSV is used later in the death data processing for geographic enrichment. So the download of city coordinates is part of the initial ingestion, but not processed until later. So the download_city_coords task is part of the metadata processing? Or is the city coordinates CSV fetched during ingestion and then used as is?

This is ambiguous, but according to the pipeline steps, the metadata processing includes downloading death files, nuclear CSV, thermal CSV. The city coordinates CSV is fetched during ingestion but perhaps not processed until the death data cleansing step.

So in the DAG, the city coordinates ingestion task (fetch_city_coords) is part of the initial parallel ingestion, but its processing is handled later. So the download_city_coords task may not exist; perhaps the city coordinates are fetched once and used as is. So maybe the fetch_city_coords task is a PythonOperator that downloads the CSV and stores it somewhere, and then the death_cleanse_task uses that CSV.

But according to the pipeline steps, the metadata processing includes downloading the death files, nuclear, and thermal CSVs. The city coordinates are fetched but perhaps not processed in the metadata step. So the fetch_city_coords task is part of the initial ingestion, and its output is used later in the death data processing.

So in the DAG, the fetch_city_coords task is part of the initial parallel ingestion, and its output is used in the death_cleanse_task.

But how to model that. The death_cleanse_task would depend on fetch_city_coords and the data_processing tasks (load_death_records). But according to the pipeline steps, the death data processing path (cleanse, store, clean) depends on the branch task, which depends on the data_processing tasks.

So perhaps the death_cleanse_task needs the city coordinates data, which was fetched during ingestion. So the death_cleanse_task depends on fetch_city_coords and the branch task.

But in the dependencies, the branch_task is downstream of the data_processing tasks, which are downstream of create_tables, which are downstream of process metadata tasks (download CSV, etc.), which are downstream of ingestion tasks.

So the death_cleanse_task would depend on branch_task (which is after data_processing), and also on fetch_city_coords (from the initial ingestion). But since fetch_city_coords is part of the initial parallel ingestion, which is upstream of process metadata tasks, which are upstream of create_tables, which are upstream of data_processing tasks, which are upstream of branch_task, then death_cleanse_task would already have fetch_city_coords as an upstream task via the ingestion -> process metadata -> create tables -> data_processing -> branch_task chain.

Wait, no. The fetch_city_coords task is part of the initial ingestion, but it's not connected to the process metadata tasks unless explicitly set. For example:

The four ingestion tasks are:

fetch_nuclear_meta, fetch_thermal_meta, fetch_death_resources, fetch_city_coords.

Then process metadata tasks:

download_nuclear_csv (depends on fetch_nuclear_meta)

download_thermal_csv (depends on fetch_thermal_meta)

download_death_files (depends on fetch_death_resources)

But fetch_city_coords is not connected to any process metadata task. So the download_city_coords task is missing. But according to the pipeline steps, the city coordinates CSV is fetched during ingestion, but there's no mention of processing it in the metadata step. So perhaps the city coordinates are simply fetched and stored, and used later in the death data cleansing.

So the fetch_city_coords task is part of the initial ingestion, and its output is used in the death_cleanse_task. Therefore, death_cleanse_task must depend on fetch_city_coords and the branch_task.

But branch_task is downstream of data_processing tasks, which are downstream of create_tables, which are downstream of process metadata tasks, which are downstream of their respective ingestion tasks. So fetch_city_coords is not in that chain. Therefore, death_cleanse_task must explicitly depend on fetch_city_coords.

But how to model that. Because the branch_task is triggered after data_processing tasks, which are after create_tables, which are after process metadata tasks, which are after their respective ingestion tasks. So the fetch_city_coords task is part of the initial ingestion but not connected to the rest unless explicitly set.

So death_cleanse_task must depend on branch_task and fetch_city_coords. But in Airflow, dependencies are set using >>. So:

branch_task >> death_cleanse_task

fetch_city_coords >> death_cleanse_task

But since fetch_city_coords is part of the initial parallel ingestion, which is upstream of process metadata tasks, which are upstream of create_tables, etc., the death_cleanse_task would have two upstream tasks: branch_task and fetch_city_coords.

But that's possible. So in code:

death_cleanse_task = PythonOperator(
    task_id='cleanse_death_records',
    python_callable=cleanse_death_records,
    op_kwargs={'city_coords_path': '/path/to/city_coords.csv'},
    dag=dag,
)

Then, in dependencies:

fetch_city_coords >> death_cleanse_task

branch_task >> death_cleanse_task

But branch_task is already downstream of data_processing tasks, which are downstream of create_tables, which are downstream of process metadata tasks, which are downstream of their respective ingestion tasks. So fetch_city_coords is part of the initial ingestion, but not connected to the rest of the process unless explicitly.

This could create a situation where death_cleanse_task depends on fetch_city_coords and branch_task, but fetch_city_coords is part of the initial parallel ingestion, which is after start_pipeline. So the dependencies are correctly set.

Now, putting all together.

The code structure would be:

- Import necessary modules: DAG, PythonOperator, BranchPythonOperator, DummyOperator, TaskGroup, etc.
- Define default_args with retries, retry_delay, etc.
- Use DAG context manager.
- Define tasks using PythonOperator and others.
- Set dependencies using >>.

Now, implementing the functions. Since the code must be executable, the functions called by PythonOperator must exist. But since the user wants the code to be complete, I'll define placeholder functions with pass or minimal logic.

For example:

def fetch_nuclear_metadata(**kwargs):
    pass

def download_nuclear_csv(**kwargs):
    pass

Similarly for other functions.

The BranchPythonOperator's function would check if death data exists. For example:

def check_death_data_availability(**kwargs):
    # Logic to check if death data exists
    # For example, check a file or variable
    if death_data_exists:
        return 'cleanse_death_records'
    else:
        return 'staging_completion_skip'

But since the actual logic isn't provided, the function returns one of the task IDs based on a placeholder condition.

Now, organizing the code with TaskGroups for logical grouping. The user mentioned using task groups for ingestion and staging phases.

So:

with TaskGroup('ingestion_phase') as ingestion_group:
    # Define the four ingestion tasks here

But perhaps it's simpler to use TaskGroups for ingestion and staging. However, the code must be executable, so proper imports are needed.

But the user didn't mention TaskGroups in the pipeline description's orchestration section, but under "Includes task groups for logical grouping of ingestion and staging phases". So yes, TaskGroups should be used.

So, the ingestion tasks (four parallel) are grouped into an ingestion_phase TaskGroup. Similarly, the staging phase tasks (create tables, data processing, branching, etc.) are in another TaskGroup.

But how to structure that. Let's see:

with DAG(...) as dag:
    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup('ingestion_phase') as ingestion_group:
        fetch_nuclear_meta = PythonOperator(...)
        fetch_thermal_meta = PythonOperator(...)
        fetch_death_resources = PythonOperator(...)
        fetch_city_coords = PythonOperator(...)

    # Then the metadata processing tasks
    with TaskGroup('metadata_processing') as metadata_group:
        download_nuclear_csv = PythonOperator(...)
        download_thermal_csv = PythonOperator(...)
        download_death_files = PythonOperator(...)

    # Create tables
    with TaskGroup('table_creation') as table_group:
        create_deaths_table = PythonOperator(...)
        create_power_plants_table = PythonOperator(...)

    # Data processing
    with TaskGroup('data_processing') as data_proc_group:
        load_death_records = PythonOperator(...)
        clean_thermal_data = PythonOperator(...)
        clean_nuclear_data = PythonOperator(...)

    # Branching
    branch_task = BranchPythonOperator(...)

    # Death data path
    with TaskGroup('death_data_processing') as death_group:
        cleanse_death = PythonOperator(...)
        store_death = PythonOperator(...)
        clean_temp = PythonOperator(...)

    # Staging completion skip
    staging_completion_skip = DummyOperator(...)

    # Power plant processing
    with TaskGroup('power_plant_processing') as power_plant_group:
        generate_sql = PythonOperator(...)
        store_plants = PythonOperator(...)

    # Death data final
    death_data_final = DummyOperator(...)

    # Staging completion
    staging_completion = DummyOperator(...)

Then dependencies:

start_pipeline >> ingestion_group

ingestion_group >> metadata_group

metadata_group >> table_group

table_group >> data_proc_group

data_proc_group >> branch_task

branch_task >> [cleanse_death, staging_completion_skip]

cleanse_death >> store_death >> clean_temp

clean_temp >> death_data_final

staging_completion_skip >> death_data_final

branch_task >> power_plant_group

power_plant_group >> staging_completion

death_data_final >> staging_completion

But also, the death_cleanse_task depends on fetch_city_coords. So:

fetch_city_coords >> cleanse_death

But fetch_city_coords is inside the ingestion_group TaskGroup. So:

ingestion_group's fetch_city_coords task >> cleanse_death

But in Airflow, when using TaskGroups, the tasks inside the group have task_ids like 'ingestion_phase.fetch_nuclear_meta', so referencing them requires using their full task ID. Alternatively, if using the variable names correctly, it should work.

Wait, when you create a task inside a TaskGroup, the task_id is prefixed with the group's ID. For example, if the TaskGroup is 'ingestion_phase', then the fetch_nuclear_meta task's ID is 'ingestion_phase.fetch_nuclear_meta'. But when you assign the task to a variable (fetch_nuclear_meta), that variable refers to the task object, which knows its own ID. So dependencies can be set using the variables directly.

So in code:

fetch_city_coords >> cleanse_death

is valid, assuming fetch_city_coords is the PythonOperator inside the ingestion_group.

But cleanse_death is inside the death_data_processing TaskGroup. So:

cleanse_death = PythonOperator(task_id='cleanse_death_records', ...)

But when inside a TaskGroup, the task_id is prefixed. So:

with TaskGroup('death_data_processing') as death_group:
    cleanse_death = PythonOperator(task_id='cleanse_death_records', ...)

The full task ID is 'death_data_processing.cleanse_death_records'.

But when setting dependencies, you can use the variable cleanse_death directly.

So, in the code, dependencies can be set between tasks regardless of their groups.

Now, considering all this, the code would be structured with TaskGroups for ingestion, metadata processing, table creation, data processing, death data processing, power plant processing.

But perhaps the user expects TaskGroups for ingestion and staging phases. The pipeline steps mention "staging phase" as part of the orchestration. So maybe:

- ingestion_phase (four tasks)
- staging_phase (metadata processing, table creation, data processing, branching, etc.)

But the pipeline steps mention "staging phase" as part of the orchestration, so perhaps the staging phase includes multiple TaskGroups.

Alternatively, the user may want to group ingestion and staging phases as separate TaskGroups.

But given the complexity, perhaps it's better to omit TaskGroups for simplicity unless explicitly required. The pipeline description mentions "Includes task groups for logical grouping of ingestion and staging phases", so TaskGroups should be used.

So:

with DAG(...) as dag:
    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup('ingestion_phase') as ingestion_group:
        fetch_nuclear_meta = PythonOperator(
            task_id='fetch_nuclear_metadata',
            python_callable=fetch_nuclear_metadata,
            dag=dag,
        )
        fetch_thermal_meta = PythonOperator(
            task_id='fetch_thermal_metadata',
            python_callable=fetch_thermal_metadata,
            dag=dag,
        )
        fetch_death_resources = PythonOperator(
            task_id='fetch_death_resources',
            python_callable=fetch_death_resources_list,
            dag=dag,
        )
        fetch_city_coords = PythonOperator(
            task_id='fetch_city_coordinates',
            python_callable=fetch_city_coordinates_csv,
            dag=dag,
        )

    with TaskGroup('metadata_processing') as metadata_group:
        download_nuclear_csv = PythonOperator(
            task_id='download_nuclear_csv',
            python_callable=download_nuclear_plant_csv,
            dag=dag,
        )
        download_thermal_csv = PythonOperator(
            task_id='download_thermal_csv',
            python_callable=download_thermal_plant_csv,
            dag=dag,
        )
        download_death_files = PythonOperator(
            task_id='download_death_files',
            python_callable=download_death_record_files,
            dag=dag,
        )

        # Dependencies within metadata_group
        fetch_nuclear_meta >> download_nuclear_csv
        fetch_thermal_meta >> download_thermal_csv
        fetch_death_resources >> download_death_files

    with TaskGroup('table_creation') as table_group:
        create_deaths_table = PythonOperator(
            task_id='create_deaths_table',
            python_callable=create_postgres_deaths_table,
            dag=dag,
        )
        create_power_plants_table = PythonOperator(
            task_id='create_power_plants_table',
            python_callable=create_postgres_power_plants_table,
            dag=dag,
        )

        # Tables depend on metadata processing
        download_death_files >> create_deaths_table
        [download_nuclear_csv, download_thermal_csv] >> create_power_plants_table

    with TaskGroup('data_processing') as data_proc_group:
        load_death_records = PythonOperator(
            task_id='load_death_records_redis',
            python_callable=load_death_records_to_redis,
            dag=dag,
        )
        clean_thermal_data = PythonOperator(
            task_id='clean_thermal_plant_data',
            python_callable=clean_thermal_plant_data,
            dag=dag,
        )
        clean_nuclear_data = PythonOperator(
            task_id='clean_nuclear_plant_data',
            python_callable=clean_nuclear_plant_data,
            dag=dag,
        )

        # Dependencies: tables must be created before data processing
        create_deaths_table >> load_death_records
        create_power_plants_table >> [clean_thermal_data, clean_nuclear_data]

    branch_task = BranchPythonOperator(
        task_id='check_death_data_availability',
        python_callable=check_death_data_availability,
        dag=dag,
    )

    # Death data processing path
    with TaskGroup('death_data_processing') as death_group:
        cleanse_death = PythonOperator(
            task_id='cleanse_death_records',
            python_callable=cleanse_death_records,
            op_kwargs={'city_coords_path': '/data/city_coords.csv'},
            dag=dag,
        )
        store_death = PythonOperator(
            task_id='store_death_records_postgres',
            python_callable=store_death_records_postgres,
            dag=dag,
        )
        clean_temp = PythonOperator(
            task_id='clean_temp_death_files',
            python_callable=clean_temporary_death_files,
            dag=dag,
        )

        cleanse_death >> store_death >> clean_temp

    staging_completion_skip = DummyOperator(
        task_id='staging_completion_skip',
        dag=dag,
    )

    # Power plant processing
    with TaskGroup('power_plant_processing') as power_plant_group:
        generate_sql = PythonOperator(
            task_id='generate_sql_queries',
            python_callable=generate_power_plant_sql_queries,
            dag=dag,
        )
        store_plants = PythonOperator(
            task_id='store_power_plants_postgres',
            python_callable=store_power_plants_postgres,
            dag=dag,
        )

        generate_sql >> store_plants

    death_data_final = DummyOperator(
        task_id='death_data_final',
        dag=dag,
    )

    staging_completion = DummyOperator(
        task_id='staging_completion',
        dag=dag,
    )

    # Set up dependencies
    start_pipeline >> ingestion_group
    ingestion_group >> metadata_group
    metadata_group >> table_group
    table_group >> data_proc_group
    data_proc_group >> branch_task

    branch_task >> [cleanse_death, staging_completion_skip]
    branch_task >> power_plant_group

    # Death data path dependencies
    cleanse_death >> store_death >> clean_temp >> death_data_final
    staging_completion_skip >> death_data_final

    # Power plant processing dependencies
    power_plant_group >> staging_completion

    # Final staging completion depends on both paths
    death_data_final >> staging_completion

    # City coordinates dependency for cleanse_death
    fetch_city_coords >> cleanse_death

Wait, but fetch_city_coords is part of the ingestion_group, which is upstream of metadata_group, etc. So the dependency fetch_city_coords >> cleanse_death is valid, as fetch_city_coords is a task in ingestion_group, and cleanse_death is in death_group.

This ensures that cleanse_death has the city coordinates data available.

Now, the functions called by the PythonOperators need to be defined. Since the user wants executable code, I'll include placeholder functions with appropriate parameters.

For example:

def fetch_nuclear_metadata():
    """Fetch nuclear power plant metadata from data.gouv.fr API."""
    pass

Similarly for other functions.

The BranchPythonOperator's function:

def check_de