<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let's start by understanding the requirements.

First, the DAG is for cleaning up old Airflow log files. The main steps are a start task, followed by parallel BashOperator tasks for log cleanup. Each worker task uses a lock file, handles configuration, and performs a three-phase cleanup. There's also scheduling and configuration through Airflow variables and DAG parameters.

The constraints mention using Airflow 2.x imports, so I'll make sure to import DAG and necessary operators correctly. The DAG needs a schedule interval of @daily, start date one day ago, catchup disabled, retries, email alerts, and be active when created.

Starting with the default_args. The user wants retries (1 retry with 1-minute delay), email on failure, and possibly a start date. Let's set default_args with these parameters. The start_date should be days_ago(1), and catchup=False in the DAG args.

The first task is an EmptyOperator named 'start'. Then, multiple parallel BashOperator tasks. Since the number of workers isn't specified, maybe the user expects a way to configure it. But the example uses three workers as a default. Wait, the description says "parallel cleanup processes across multiple worker nodes"—maybe the number of workers depends on the environment, but the example uses three tasks for demonstration. So perhaps create three BashOperator tasks, each processing different directories. Alternatively, use a loop if dynamic, but the example might use static tasks for simplicity.

Each BashOperator should execute a bash script that does the three-phase cleanup. The script needs to handle locking with /tmp/airflow_log_cleanup_worker.lock. So each task should create a lock file, perform the cleanup steps, then release the lock. But using a lock file across parallel tasks might cause contention. Wait, the description says "file-based locking to prevent concurrent execution conflicts". So each worker task should check for the lock file before proceeding. If the lock exists, maybe wait. But how to handle that in Bash? Maybe using a while loop with a sleep until the lock is acquired. Alternatively, use a random sleep to stagger execution as per the configurable sleep time.

The configuration retrieval: DAG run parameters or Airflow variables. So in the Bash script, we can retrieve max_log_age from the DAG's conf or Airflow variable. Similarly for enable_delete_child_log. The script should check if the DAG run has a parameter, else use the Airflow variable.

The three-phase cleanup steps:

1. Delete old log files using find with mtime.
2. Remove empty subdirs.
3. Remove empty top-level dirs.

The Bash command would need to:

- Acquire the lock (maybe using a random sleep to stagger)
- Use find commands to delete files older than max_log_age days.
- Then remove empty directories.

Also, handle errors and ensure the lock is cleaned up on failure. Using a trap in the bash script might help.

Putting this into a BashOperator's bash_command. Let's construct the command.

The script would look something like:

LOCK_FILE="/tmp/airflow_log_cleanup_worker.lock"
SLEEP_TIME=$((RANDOM % 30))  # Random sleep up to 30 seconds
sleep $SLEEP_TIME

# Attempt to acquire lock
if mkdir $LOCK_FILE; then
    trap 'rm -rf $LOCK_FILE' EXIT
    # Proceed with cleanup
    max_age=${max_log_age_in_days:-7}  # Get from DAG conf or variable
    # Phase 1: Delete old files
    find $BASE_LOG_FOLDER -type f -mtime +$max_age -delete
    # Phase 2: Remove empty subdirs
    find $BASE_LOG_FOLDER -mindepth 1 -type d -empty -delete
    # Phase 3: Remove empty top-level dirs (if enabled)
    if [ "$ENABLE_DELETE_CHILD_LOG" = "true" ]; then
        find $BASE_LOG_FOLDER -maxdepth 1 -type d -empty -delete
    fi
else
    echo "Could not acquire lock, exiting"
    exit 1
fi

Wait, but the lock is per worker? Or per task? The description says "file-based locking to prevent concurrent execution conflicts". So each worker task should check for the lock. But if multiple tasks are running in parallel, each trying to create the same lock file, that might not work. Wait, using mkdir as a lock mechanism because it's atomic. So if multiple tasks try to create the same directory (LOCK_FILE), only one will succeed. The others will fail and exit. But that would mean only one worker task actually does the cleanup, which contradicts the parallel processing. Hmm, maybe the lock is per worker node, but the example uses a single lock file. That might not be correct. Alternatively, perhaps each worker should process a subset of directories, and the lock is per directory. But the description isn't clear. The original pipeline says "parallel cleanup processes across multiple worker nodes", so maybe each worker node has its own lock. But in the example, the lock file is named the same for all tasks, which would serialize the tasks instead of allowing parallelism. That's a problem.

Wait, maybe the lock file is per worker, but the example uses a single lock file. That would cause the parallel tasks to contend for the same lock, effectively making them run sequentially. That's not desired. So perhaps the lock file name should include a unique identifier, like the worker's hostname or task instance. But the description says "file-based locking using /tmp/airflow_log_cleanup_worker.lock". So maybe the lock is per worker node, assuming each worker node has its own /tmp directory. But if tasks are running on the same worker, the lock would prevent concurrent execution. However, if tasks are distributed across workers, each worker has its own lock file. So the lock is per worker node. That makes sense. So each worker node can run one cleanup task at a time, but multiple nodes can run in parallel. So the lock is local to the worker's /tmp directory.

In that case, the lock file path is local to each worker, so multiple tasks on different workers can proceed in parallel. So the lock is per worker node, not global. That makes sense. So the Bash script can safely use the lock file path, as each worker's /tmp is separate.

So the Bash command would:

- Sleep a random time (configurable) to stagger execution.
- Attempt to create the lock file (using mkdir, which is atomic).
- If successful, proceed with cleanup; else exit.

Now, the configuration parameters: max_log_age_in_days comes from DAG run conf or Airflow variable. Similarly for enable_delete_child_log.

In the BashOperator, we can access Airflow variables using the jinja template. For example, to get the variable 'airflow_log_cleanup__max_log_age_in_days', we can use '{{ var.value.get("airflow_log_cleanup__max_log_age_in_days", 7) }}'. But the DAG run conf can override this. Wait, how to pass the DAG run conf to the BashOperator? The conf is available in the context, so in the Bash command, we can check if the conf parameter exists. For example:

max_age_in_days="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"

But in Bash, we can't directly use Jinja variables. Wait, the BashOperator's bash_command is a template, so we can use Jinja variables. So inside the bash_command string, we can use {{ ... }} to access variables.

So the Bash script would retrieve the max_log_age_in_days from the DAG run's conf, else the Airflow variable, else default to 7.

Similarly for enable_delete_child_log: check the Airflow variable 'airflow_log_cleanup__enable_delete_child_log'.

Putting this all together, the Bash command would be a multi-line string using Jinja templating.

Now, the three-phase cleanup:

Phase 1: find command to delete files older than max_log_age_in_days.

Phase 2: find and delete empty subdirectories.

Phase 3: if enabled, delete empty top-level directories.

The BASE_LOG_FOLDER comes from Airflow's configuration, which is available as a setting. So in the Bash script, we can access it via $AIRFLOW_HOME/logs, but better to get it from the Airflow configuration. Alternatively, the BashOperator can retrieve it via '{{ conf.get("core", "BASE_LOG_FOLDER") }}'.

Wait, in Airflow, the configuration is accessible via the context, but in BashOperator, we can use Jinja to get the configuration value. So:

BASE_LOG_FOLDER="{{ conf.get('core', 'BASE_LOG_FOLDER') }}"

Similarly, the optional scheduler child process log directory: perhaps part of the BASE_LOG_FOLDER or another path. The description says "optional scheduler child process log directory", so maybe another variable. But the example might not include it unless specified. The user might just include the BASE_LOG_FOLDER.

Putting this all into the Bash command.

Now, handling errors: the trap command in Bash to ensure the lock is removed on exit, even if the script fails.

So the Bash script would look like:

#!/usr/bin/env bash

LOCK_FILE="/tmp/airflow_log_cleanup_worker.lock"
SLEEP_TIME=$(( RANDOM % {{ var.value.get("airflow_log_cleanup__sleep_time", 30) }} ))
sleep ${SLEEP_TIME}

if mkdir "${LOCK_FILE}" 2>/dev/null; then
    trap 'rm -rf "${LOCK_FILE}"' EXIT

    MAX_AGE_DAYS="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"
    BASE_LOG_FOLDER="{{ conf.get('core', 'BASE_LOG_FOLDER') }}"
    ENABLE_CHILD_LOG_DELETE="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

    # Phase 1: Delete old log files
    find "${BASE_LOG_FOLDER}" -type f -mtime "+${MAX_AGE_DAYS}" -delete

    # Phase 2: Remove empty subdirectories
    find "${BASE_LOG_FOLDER}" -mindepth 1 -type d -empty -delete

    # Phase 3: Remove empty top-level directories if enabled
    if [[ "${ENABLE_CHILD_LOG_DELETE}" == "true" ]]; then
        find "${BASE_LOG_FOLDER}" -maxdepth 1 -type d -empty -delete
    fi
else
    echo "Failed to acquire lock, exiting."
    exit 1
fi

But need to handle cases where the directory doesn't exist, etc. Also, the SLEEP_TIME is mentioned as configurable; perhaps the Airflow variable 'airflow_log_cleanup__sleep_time' with a default of 30.

But the pipeline description mentions "configurable sleep time staggers worker execution to distribute load". So the sleep time can be set via an Airflow variable, defaulting to 30 seconds. So in the Bash script, the SLEEP_TIME is set to a random value up to the configured sleep time.

Now, the BashOperator tasks. Since they are parallel, we can create multiple instances. The example uses three workers, perhaps as a default. So create three BashOperator tasks, each named like 'cleanup_worker_1', 'cleanup_worker_2', etc. But how to parameterize them? Maybe each processes different log directories, but the description says each worker processes one or more log directories. However, the example might not specify, so perhaps each worker processes the same directories but uses locking to prevent conflicts. Wait, but the lock is per worker node. So if multiple tasks are scheduled on the same worker, they would contend for the lock. But if tasks are spread across workers, they can run in parallel.

Alternatively, perhaps each worker task is responsible for a subset of directories. But the pipeline description doesn't specify how to split the directories. So maybe the example uses a single log directory (BASE_LOG_FOLDER) and each worker processes it, but with locking. But that would mean only one worker per node can run the cleanup. However, the description says "parallel cleanup processes across multiple worker nodes", implying that multiple workers can run on different nodes. So each worker node can run its own cleanup, processing all log directories. But that might lead to redundant work. Hmm, perhaps the example is simplified, and each worker task is responsible for a different subset, but the code doesn't handle that. Alternatively, the code uses multiple workers for demonstration, but in reality, the number of workers would depend on the setup.

Given the ambiguity, perhaps create three BashOperator tasks as parallel workers, each executing the same script. The lock file ensures that on each worker node, only one task runs at a time, but multiple nodes can run in parallel.

So, in the DAG code, create three BashOperator tasks (maybe as an example), named cleanup_worker_1, cleanup_worker_2, cleanup_worker_3, all depending on the start task, and then perhaps a final task that runs after all workers complete.

Wait, the pipeline steps mention "parallel log cleanup tasks" in a fan-out pattern. So after the start task, multiple workers run in parallel, and there's no mention of a join step. So the DAG would have start >> [worker1, worker2, worker3]. But the description doesn't mention any task after the parallel workers. So maybe the DAG ends after all workers complete.

But the pipeline's orchestration says "retry policy: 1 retry with 1-minute delay". So each task has retries.

Now, putting it all together.

Imports needed: DAG, BashOperator, EmptyOperator, PythonOperator (if needed), etc. Also, maybe the datetime and timedelta for start_date.

Default_args would include email_on_failure, retries, retry_delay, etc.

The DAG is named something like 'log_cleanup_dag', schedule_interval='@daily', start_date=days_ago(1), catchup=False, default_args=default_args, tags maybe.

Now, code:

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'admin@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='log_cleanup_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['maintenance'],
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(task_id='start')

    cleanup_commands = """
        LOCK_FILE="/tmp/airflow_log_cleanup_worker.lock"
        SLEEP_TIME=$(( RANDOM % {{ var.value.get("airflow_log_cleanup__sleep_time", 30) }} ))
        sleep ${SLEEP_TIME}

        if mkdir "${LOCK_FILE}" 2>/dev/null; then
            trap 'rm -rf "${LOCK_FILE}"' EXIT

            MAX_AGE_DAYS="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"
            BASE_LOG_FOLDER="{{ conf.get('core', 'BASE_LOG_FOLDER') }}"
            ENABLE_CHILD_LOG_DELETE="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

            # Phase 1: Delete old files
            find "${BASE_LOG_FOLDER}" -type f -mtime "+${MAX_AGE_DAYS}" -delete

            # Phase 2: Remove empty subdirectories
            find "${BASE_LOG_FOLDER}" -mindepth 1 -type d -empty -delete

            # Phase 3: Remove top-level directories if enabled
            if [[ "${ENABLE_CHILD_LOG_DELETE}" == "true" ]]; then
                find "${BASE_LOG_FOLDER}" -maxdepth 1 -type d -empty -delete
            fi
        else
            echo "Failed to acquire lock. Exiting."
            exit 1
        fi
    """

    # Create three parallel workers as an example
    workers = []
    for i in range(3):
        task = BashOperator(
            task_id=f'cleanup_worker_{i+1}',
            bash_command=cleanup_commands,
            retries=default_args['retries'],
            retry_delay=default_args['retry_delay'],
        )
        workers.append(task)

    start >> workers

Wait, but the email in default_args is set to 'admin@example.com', but the user might want to have their own email. However, the pipeline description says "email alerts configured for failure conditions", so the code should include email settings. Since the example uses a placeholder email, that's acceptable.

The Bash command uses a multi-line string. The BashOperator will execute this script. The script uses Airflow variables and configuration.

Each worker task (cleanup_worker_1, etc.) runs the same script. The lock file ensures that on each worker node, only one task runs at a time. If multiple tasks are scheduled on the same worker, they will contend for the lock. However, if tasks are distributed across workers, they can run in parallel.

The SLEEP_TIME is randomized using the Airflow variable 'airflow_log_cleanup__sleep_time', defaulting to 30 seconds. This staggers the workers' execution.

The script checks for the DAG run parameter 'maxLogAgeInDays', then the Airflow variable 'airflow_log_cleanup__max_log_age_in_days', defaulting to 7 days.

The ENABLE_CHILD_LOG_DELETE is retrieved from the Airflow variable 'airflow_log_cleanup__enable_delete_child_log', defaulting to 'false'.

The three find commands perform the deletion steps.

Error handling: The trap ensures the lock is removed on script exit, whether successful or not.

The DAG is scheduled daily, starts one day ago, catchup disabled, and is active upon creation.

This should meet all the requirements. Now, check for PEP8 compliance, proper imports, and correct task dependencies.

Imports: from airflow.operators.empty import EmptyOperator (correct in Airflow 2.x). BashOperator from airflow.operators.bash.

The default_args include 'owner', 'depends_on_past' as False, email settings, retries, retry_delay.

The DAG uses the context manager, and tasks are defined within it.

The workers are created in a loop (three tasks), each as a BashOperator with the same command.

Dependencies: start >> workers (using list expansion).

This should generate a valid DAG.
</think>

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'admin@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1)
}

with DAG(
    dag_id='airflow_log_cleanup',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['maintenance'],
    is_paused_upon_creation=False
) as dag:

    start = EmptyOperator(task_id='start')

    cleanup_script = """
        LOCK_FILE="/tmp/airflow_log_cleanup_worker.lock"
        SLEEP_TIME=$(( RANDOM % {{ var.value.get("airflow_log_cleanup__sleep_time", 30) }} ))
        sleep ${SLEEP_TIME}

        if mkdir "${LOCK_FILE}" 2>/dev/null; then
            trap 'rm -rf "${LOCK_FILE}"' EXIT
            
            MAX_AGE_DAYS="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"
            BASE_LOG_FOLDER="{{ conf.get('core', 'BASE_LOG_FOLDER') }}"
            ENABLE_CHILD_LOG_DELETE="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

            echo "Cleaning logs older than ${MAX_AGE_DAYS} days in ${BASE_LOG_FOLDER}"
            find "${BASE_LOG_FOLDER}" -type f -mtime "+${MAX_AGE_DAYS}" -delete
            
            echo "Removing empty subdirectories"
            find "${BASE_LOG_FOLDER}" -mindepth 1 -type d -empty -delete

            if [[ "${ENABLE_CHILD_LOG_DELETE}" == "true" ]]; then
                echo "Removing empty top-level directories"
                find "${BASE_LOG_FOLDER}" -maxdepth 1 -type d -empty -delete
            fi
        else
            echo "Could not acquire lock - another cleanup process is running"
            exit 0
        fi
    """

    worker_tasks = [
        BashOperator(
            task_id=f'cleanup_worker_{i}',
            bash_command=cleanup_script,
            retries=1,
            retry_delay=timedelta(minutes=1)
        ) for i in range(1, 4)
    ]

    start >> worker_tasks