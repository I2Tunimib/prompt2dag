<think>
Okay, I need to convert the given pipeline description into an Apache Airflow DAG. Let's start by understanding the requirements and constraints.

First, the pipeline is for cleaning up old Airflow log files. The main steps are: a start task, parallel cleanup tasks using BashOperator, and proper configuration handling. The DAG should run daily, disable catchup, have retries, and email alerts.

The user specified using Airflow 2.x style imports. So I'll import DAG from airflow, PythonOperator and BashOperator from airflow.operators, and maybe others like variables. Also, since there's a mention of parallel execution, I'll need to use something like the TaskGroup or just set dependencies correctly for parallelism.

Wait, the parallel log cleanup tasks are multiple BashOperator tasks that fan out. Each worker processes log directories. But how to represent that? Maybe create multiple Bash tasks dynamically based on some configuration. However, the example might need a fixed number for simplicity. Alternatively, use a loop to generate tasks for each directory. But the problem says "each worker processes one or more log directories"—maybe the actual directories are determined at runtime, perhaps based on BASE_LOG_FOLDER and maybe subdirectories. But for the code example, perhaps the user expects a fixed number of parallel tasks, maybe using a list of directories or just a parameterized Bash command that handles all directories. Hmm.

Wait, the description says "parallel cleanup processes across multiple worker nodes". So maybe each Bash task is responsible for a subset of directories. But without specific info, perhaps the code should have a loop that creates multiple BashOperators, each handling a different log directory. But how to get the list of directories? The BASE_LOG_FOLDER is from Airflow's configuration. Maybe the code retrieves that and lists subdirectories. But that might complicate the example. Alternatively, perhaps the example uses a fixed number of workers, like three, each processing a directory. But the user might expect a more generic approach.

Alternatively, maybe the parallel tasks are not per directory but per worker node, and each task uses a lock file to coordinate. The lock file is in /tmp/airflow_log_cleanup_worker.lock. So each Bash task would try to acquire the lock, do the cleanup, then release. But that's more about concurrency control. However, the problem states "parallel cleanup processes across multiple worker nodes", which suggests that each task runs on a different worker and uses a lock file to prevent conflicts. So each BashOperator task would execute the same script but with a lock file to ensure only one runs per worker node? Or perhaps each task is responsible for a specific log directory, and the lock is per directory.

This part is a bit unclear. The description mentions "file-based locking using /tmp/airflow_log_cleanup_worker.lock". So each worker (task) creates a lock file to prevent concurrent execution. So the Bash script would check for the existence of the lock file, create it, perform the cleanup, then remove it. If the task fails, the lock file is cleaned up. So each Bash task would include steps to handle the lock.

The configuration comes from DAG run parameters or Airflow variables. So the max log age can be set via an Airflow Variable or a DAG parameter. The code should retrieve these, perhaps using a Python callable to get the configuration.

The three-phase cleanup process involves deleting old files, removing empty subdirs, and removing top-level dirs. The Bash commands would use find with appropriate options. For example:

find $log_dir -type f -mtime +$max_age -delete
find $log_dir -type d -empty -delete
find $log_dir -type d -empty -delete (maybe run twice to handle nested dirs?)

But the exact commands might vary. Also, the sleep time is configurable to stagger execution.

The DAG should have a start task (EmptyOperator), then multiple parallel Bash tasks, then maybe an end task. But the description doesn't mention an end task, just the start and the parallel tasks. So dependencies would be start_task >> [cleanup_task1, cleanup_task2, ...].

Now, considering the Airflow variables: airflow_log_cleanup__max_log_age_in_days and airflow_log_cleanup__enable_delete_child_log. The code needs to fetch these variables, with a default if not set. Also, the DAG run configuration can override maxLogAgeInDays.

The Bash script would need to handle these variables. So perhaps in the Bash command, we retrieve the variables using Airflow's macros, like {{ var.value.get(...) }}. Alternatively, use a PythonOperator to prepare the parameters and pass them to the BashOperator.

But since the BashOperator is executing a command, maybe it's better to retrieve the variables within the Bash script using airflow variables command, but that's not efficient. Alternatively, use Jinja templating to pass the variables into the Bash command.

Wait, the Airflow variables can be accessed in the Bash command via Jinja. For example:

bash_command="echo {{ var.value.get('airflow_log_cleanup__max_log_age_in_days', 7) }}"

But also, the DAG run configuration can override this. So the code should first check if the parameter was passed in the DAG run, else use the variable, else default. That requires a Python callable to prepare the parameters, perhaps using a PythonOperator that pushes the parameters to XCom, then the BashOperators pull them.

Alternatively, the Bash command can be templated to check both the DAG run conf and the variables. But that might complicate the command.

Alternatively, create a function that retrieves the max log age, considering both the DAG run conf and the Airflow variable, and pass that as an argument to the BashOperator.

So perhaps a PythonOperator as a setup task that retrieves the configuration and pushes it to XCom, then the BashOperators pull from XCom.

But the pipeline steps mention that each worker retrieves configuration from DAG run parameters or Airflow variables. So maybe each Bash task has to handle that. But how?

Alternatively, the code can have a default_args that includes 'params' with maxLogAgeInDays, and then in the Bash command, access it via {{ params.maxLogAgeInDays }}. But if the DAG run is triggered with a configuration that includes this parameter, it would override the default. Also, the Airflow variable would be a fallback.

Wait, the DAG run configuration can be accessed via {{ dag_run.conf.get('maxLogAgeInDays', default_value) }}. So the Bash command could first check dag_run.conf, then the Airflow variable, then a default.

But combining all that into a Bash command might be messy. Alternatively, use a PythonOperator to handle the configuration and pass it as an argument to the Bash tasks.

Let me outline the steps:

1. Start task: EmptyOperator.

2. A setup task (PythonOperator) that retrieves the max_log_age_in_days from either the DAG run conf, the Airflow variable, or a default. Similarly for enable_delete_child_log. Then push these values to XCom.

3. Multiple BashOperator tasks that run in parallel. Each task executes the cleanup script, using the parameters from XCom.

But how to handle the lock file. Each Bash task would need to create a lock file, perhaps with a unique name based on the task instance or worker node. But the problem states the lock file is /tmp/airflow_log_cleanup_worker.lock. Wait, but if multiple tasks are running in parallel on the same worker, they would contend for the same lock file. So perhaps each task uses the same lock file, ensuring that only one cleanup process runs per worker node at a time.

The Bash script would need to check for the lock file, create it, perform cleanup, then delete it. Also, handle cases where the task fails to ensure the lock is removed.

So the Bash command could be something like:

lock_file="/tmp/airflow_log_cleanup_worker.lock"

# Try to create lock file
if mkdir "$lock_file"; then
    # Perform cleanup steps
    find ...;
    find ...;
    find ...;
    # Remove lock
    rmdir "$lock_file"
else
    echo "Another cleanup is running, exiting"
    exit 0
fi

Wait, using mkdir as an atomic operation. Because creating a directory is atomic, unlike touching a file. So using mkdir to create the lock directory. That way, only one process can create it.

But in the problem description, the lock file is named .lock, but perhaps using a directory is better. Alternatively, use a file and check existence. But using a directory is safer.

So the Bash script would:

- Check if the lock directory exists. If not, create it and proceed.

- On exit, remove the lock directory.

But handling errors: if the task fails, the lock directory remains. So need to ensure it's cleaned up on failure. Perhaps using a trap in the Bash script.

So the Bash command would be:

cleanup_command = """
lock_dir="/tmp/airflow_log_cleanup_worker.lock"
if mkdir "$lock_dir"; then
    trap 'rm -rf "$lock_dir"' EXIT
    sleep {{ params.sleep_time }}

    max_age={{ params.max_log_age }}
    base_log_dir="{{ params.base_log_folder }}"

    # Delete old log files
    find "$base_log_dir" -type f -mtime "+$max_age" -delete

    # Delete empty child log directories if enabled
    {% if params.delete_child_log %}
    find "$base_log_dir" -mindepth 1 -type d -empty -delete
    {% endif %}

    # Delete empty top-level directories
    find "$base_log_dir" -type d -empty -delete

else
    echo "Cleanup already in progress on this worker, skipping"
    exit 0
fi
"""

But how to handle the parameters. The params would come from the setup task or from the DAG's parameters.

Wait, the DAG's default_args can include a 'params' section, but the DAG run can override them. Alternatively, retrieve the parameters in a Python function and pass them to the BashOperators.

Alternatively, use Jinja templating to access variables and DAG run parameters directly in the Bash command.

For example:

max_age = "{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"

But this would be part of the Bash command's Jinja template.

Similarly for enable_delete_child_log: check the Airflow variable.

But the problem says that the DAG run configuration can override the maxLogAgeInDays parameter. So in the Bash command, we can first check if the DAG run conf has 'maxLogAgeInDays', else use the Airflow variable, else default to 7.

So in the Bash command, the max_age would be set as:

max_age="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"

Similarly for enable_delete_child_log, which is a boolean. But in Jinja, we can check if the variable exists and is true.

But Airflow variables are stored as strings, so need to handle that. For example:

{% if var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') | lower == 'true' %} ... {% endif %}

But also, the DAG run configuration could override this. Wait, the problem says the DAG run configuration has a parameter 'maxLogAgeInDays', but for enable_delete_child_log, it's only via the Airflow variable. Wait, the description says:

"Airflow Variables: airflow_log_cleanup__enable_delete_child_log for child process log inclusion"

So the enable_delete_child_log is controlled by the Airflow variable, not the DAG run parameters. So in the Bash command, we check the variable.

Putting this all together, the Bash command would be a multi-line string with Jinja templating.

But handling all this in the BashOperator's bash_command parameter.

Now, the parallel tasks: how many? The problem says "parallel cleanup processes across multiple worker nodes". So perhaps the number of tasks corresponds to the number of worker nodes, but since Airflow tasks can run on any worker, maybe the number of tasks is arbitrary, and each task uses the lock file to prevent concurrent execution on the same worker. So even if multiple tasks are assigned to the same worker, only one would run at a time.

But how to represent this in the DAG. Maybe create multiple tasks (e.g., 3) to fan out, each using the same Bash command with the lock file. But in reality, the number of parallel tasks would depend on the number of log directories or worker nodes. However, the example might just create three parallel tasks for demonstration.

Alternatively, the code could generate tasks dynamically based on the subdirectories in BASE_LOG_FOLDER. But that would require a PythonOperator to list the directories and create tasks accordingly. For simplicity, perhaps the example uses a fixed number of tasks, say three, each processing the same log directory but with different sleep times to stagger execution.

Wait, the problem mentions "configurable sleep time staggers worker execution to distribute load". So each worker task sleeps for a certain time before starting, with the sleep time being configurable. So perhaps each Bash task has a different sleep time, like 0s, 10s, 20s, etc., to stagger their execution.

But how to pass the sleep time to each task. Maybe in a loop, creating BashOperators with different sleep_time parameters.

Putting this all together:

The DAG would start with an EmptyOperator as the start task.

Then, multiple BashOperators (say, three) are created in a loop, each with a different sleep_time parameter. These tasks are all downstream of the start task and run in parallel.

Each BashOperator's command includes the lock file handling, sleep, and cleanup steps.

The parameters (max_log_age, base_log_folder, delete_child_log) are retrieved via Jinja templating, accessing DAG run conf, Airflow variables, or defaults.

The DAG's default_args include email alerts on retry, retries=1, retry_delay=timedelta(minutes=1), etc.

The schedule_interval is '@daily', start_date is one day ago, catchup=False.

Now, let's draft the code.

First, imports:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

Then, default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

Define the DAG:

with DAG(
    dag_id='log_cleanup_dag',
    default_args=default_args,
    description='Automated cleanup of old Airflow log files',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance'],
) as dag:

    start_task = EmptyOperator(task_id='start')

Then, create multiple cleanup tasks. Let's say three workers. But how to parameterize them. Maybe use a loop:

cleanup_tasks = []
for i in range(3):
    task = BashOperator(
        task_id=f'cleanup_worker_{i}',
        bash_command="""
        lock_dir="/tmp/airflow_log_cleanup_worker.lock"
        if mkdir "$lock_dir"; then
            trap 'rm -rf "$lock_dir"' EXIT
            sleep {{ params.sleep_time }}

            max_age={{ params.max_log_age }}
            base_log_dir="{{ params.base_log_folder }}"

            # Delete old log files
            find "$base_log_dir" -type f -mtime "+$max_age" -delete

            # Delete empty child log directories if enabled
            {% if params.delete_child_log %}
            find "$base_log_dir" -mindepth 1 -type d -empty -delete
            {% endif %}

            # Delete empty top-level directories
            find "$base_log_dir" -type d -empty -delete

        else
            echo "Cleanup already in progress on this worker, skipping"
            exit 0
        fi
        """,
        params={
            'sleep_time': i * 10,  # Stagger sleep time
            'max_log_age': '{{ dag_run.conf.get("maxLogAgeInDays", var.value.get("airflow_log_cleanup__max_log_age_in_days", 7)) }}',
            'base_log_folder': '{{ conf.get("logging", "BASE_LOG_FOLDER") }}',
            'delete_child_log': '{{ var.value.get("airflow_log_cleanup__enable_delete_child_log", "false") }}',
        },
    )
    cleanup_tasks.append(task)

But wait, the params in BashOperator are passed as a dictionary, and in the bash_command, they are accessed via {{ params.param_name }}. However, the 'max_log_age' in params is a string that includes Jinja templating. But when the BashOperator renders the template, the params themselves are not templated again. So this approach might not work. Because the params are static; the code inside the params is not evaluated as Jinja.

Ah, right. The params dictionary is not templated. So the correct approach is to directly use the Jinja expressions in the bash_command, not via params.

So instead, the bash_command should directly reference variables and DAG run parameters.

Let me revise the BashOperator's bash_command:

bash_command="""
lock_dir="/tmp/airflow_log_cleanup_worker.lock"
if mkdir "$lock_dir"; then
    trap 'rm -rf "$lock_dir"' EXIT
    sleep {{ ti.xcom_pull(task_ids='get_config', key='sleep_time') }}

    max_age={{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}
    base_log_dir="{{ conf.get('logging', 'BASE_LOG_FOLDER') }}"
    enable_child_log="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

    # Delete old log files
    find "$base_log_dir" -type f -mtime "+$max_age" -delete

    # Delete empty child log directories if enabled
    if [ "$enable_child_log" = "true" ]; then
        find "$base_log_dir" -mindepth 1 -type d -empty -delete
    fi

    # Delete empty top-level directories
    find "$base_log_dir" -type d -empty -delete

else
    echo "Cleanup already in progress on this worker, skipping"
    exit 0
fi
"""

Wait, but the sleep time is per task. How to stagger them. If each task has a different sleep time, perhaps in a loop, each task gets a different sleep time. But how to pass that into the Bash command.

Alternatively, use a PythonOperator to generate the sleep times and push to XCom, then each Bash task pulls its own sleep time. But that complicates things.

Alternatively, in the loop that creates the Bash tasks, pass the sleep time as a parameter. For example:

for i in range(3):
    sleep_time = i * 10
    task = BashOperator(
        task_id=f'cleanup_worker_{i}',
        bash_command="""
        lock_dir="/tmp/airflow_log_cleanup_worker.lock"
        if mkdir "$lock_dir"; then
            trap 'rm -rf "$lock_dir"' EXIT
            sleep {{ params.sleep_time }}

            max_age={{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}
            base_log_dir="{{ conf.get('logging', 'BASE_LOG_FOLDER') }}"
            enable_child_log="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

            # Delete old log files
            find "$base_log_dir" -type f -mtime "+$max_age" -delete

            # Delete empty child log directories if enabled
            if [ "$enable_child_log" = "true" ]; then
                find "$base_log_dir" -mindepth 1 -type d -empty -delete
            fi

            # Delete empty top-level directories
            find "$base_log_dir" -type d -empty -delete

        else
            echo "Cleanup already in progress on this worker, skipping"
            exit 0
        fi
        """,
        params={'sleep_time': sleep_time},
    )
    cleanup_tasks.append(task)

This way, each task has a different sleep_time parameter. The params are accessible via {{ params.sleep_time }} in the bash_command.

But wait, the params are passed to the operator, and in the bash_command, we can use {{ params.sleep_time }}. That should work.

But the other parameters (max_age, base_log_dir, enable_child_log) are retrieved via Jinja templating.

The base_log_dir uses conf.get('logging', 'BASE_LOG_FOLDER'), which accesses Airflow's configuration.

The enable_child_log is retrieved from the Airflow variable, defaulting to 'false'.

In the Bash script, the enable_child_log is checked as a string. If it's 'true', then the child log directories are deleted.

Now, the lock directory is created using mkdir. If successful, the trap ensures it's removed on exit (success or failure). If mkdir fails (because another task is holding the lock), the task exits with 0 to avoid failure.

The three find commands:

1. Delete files older than max_age days.

2. If enabled, delete empty child directories.

3. Delete empty top-level directories.

But the third find command might delete the base_log_dir if it's empty. Wait, the third command is 'find "$base_log_dir" -type d -empty -delete', which would delete any empty directory under base_log_dir, including the base itself if empty. But the first find deletes files, then the second deletes child directories, then the third deletes any remaining empty directories, including the base. But perhaps the intention is to delete empty top-level directories under base_log_dir. Wait, the problem description says "remove empty top-level log directories". So perhaps the base_log_dir contains multiple top-level directories (like per DAG or per task), and after deleting old files, those directories might be empty and should be removed.

But the third find command would delete any empty directory within base_log_dir, including nested ones. Wait, no: 'find "$base_log_dir" -type d -empty -delete' would delete all empty directories under base_log_dir. The second find (if enabled) deletes mindepth 1, which are the child directories. Then the third find deletes any empty directories, which could include the base_log_dir if it's empty. But maybe that's intended.

But perhaps the third step is intended to remove empty top-level directories, which are immediate children of base_log_dir. So using '-mindepth 1 -maxdepth 1' in the find command. But the problem description says "remove empty top-level log directories", which are the directories directly under base_log_dir. So the third find should be:

find "$base_log_dir" -mindepth 1 -maxdepth 1 -type d -empty -delete

But the original description's third phase is "remove empty top-level log directories". So perhaps the third find is:

find "$base_log_dir" -mindepth 1 -maxdepth 1 -type d -empty -delete

But the user's example might have the original code with three phases:

1. Delete old files.

2. Remove empty subdirs (child directories).

3. Remove empty top-level directories.

So perhaps the second find is for child directories (mindepth 1), and the third is for top-level (mindepth 1 maxdepth 1). Wait, no. If base_log_dir is /logs, then top-level directories are /logs/dag1, /logs/dag2, etc. So to remove those if empty, the find command would be:

find "$base_log_dir" -mindepth 1 -maxdepth 1 -type d -empty -delete

But the problem's third phase says "remove empty top-level log directories", which are the immediate children of base_log_dir. So the third find should use -maxdepth 1.

But the original code in the problem's three-phase cleanup uses:

- find ... -delete for old files.

- find ... -empty -delete for subdirs.

- find ... -empty -delete for top-level.

So perhaps the third find is without mindepth/maxdepth, which would delete any empty directory, including the base_log_dir itself. That might not be intended. So perhaps there's a mistake here. But given the problem description, the code should reflect the user's specified steps.

In any case, the code will include the three find commands as described.

Putting it all together:

The DAG code would have a start task, followed by multiple BashOperator tasks created in a loop, each with a different sleep_time parameter. The tasks are dependent on the start task and run in parallel.

The Bash command handles locking, sleeps for the staggered time, then performs the three-phase cleanup.

The parameters for max_log_age are retrieved from DAG run conf or Airflow variable, and enable_child_log from the Airflow variable.

The DAG is scheduled daily, starts a day before the current date (but in the example, I'll set a fixed start_date like datetime(2023, 1, 1)), catchup disabled, retries 1, emails on failure.

Now, code:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='log_cleanup_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance'],
) as dag:

    start_task = EmptyOperator(task_id='start')

    cleanup_tasks = []
    for i in range(3):  # Three parallel workers
        cleanup_task = BashOperator(
            task_id=f'cleanup_worker_{i}',
            bash_command="""
            lock_dir="/tmp/airflow_log_cleanup_worker.lock"
            if mkdir "$lock_dir"; then
                trap 'rm -rf "$lock_dir"' EXIT
                sleep {{ params.sleep_time }}

                max_age={{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}
                base_log_dir="{{ conf.get('logging', 'BASE_LOG_FOLDER') }}"
                enable_child_log="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"

                # Phase 1: Delete old log files
                find "$base_log_dir" -type f -mtime "+$max_age" -delete

                # Phase 2: Remove empty child directories if enabled
                if [ "$enable_child_log" = "true" ]; then
                    find "$base_log_dir" -mindepth 1 -type d -empty -delete
                fi

                # Phase 3: Remove empty top-level directories
                find "$base_log_dir" -mindepth 1 -maxdepth 1 -type d -empty -delete

            else
                echo "Cleanup already in progress on this worker, skipping"
                exit 0
            fi
            """,
            params={'sleep_time': i * 10},
        )
        cleanup_tasks.append(cleanup_task)

    start_task >> cleanup_tasks

Wait, but in the third phase, the find command uses -mindepth 1 -maxdepth 1 to target only the immediate subdirectories of base_log_dir. That aligns with "empty top-level log directories".

Also, the trap ensures that the lock directory is removed even if the task fails.

The sleep time is staggered by multiplying the loop index by 10 seconds.

Each Bash task is named cleanup_worker_0, cleanup_worker_1, etc., and they are all downstream of the start_task.

This code should be PEP 8 compliant, uses Airflow 2.x imports, defines the DAG with the required parameters, and includes proper task dependencies.

Testing this code would require Airflow to be set up with the appropriate variables and configuration. The BASE_LOG_FOLDER should be set in airflow.cfg, and the variables airflow_log_cleanup__max_log_age_in_days and airflow_log_cleanup__enable_delete_child_log can be set via the Airflow UI.

The code doesn't include sensors since the problem description doesn't mention any. The tasks are all BashOperators and an EmptyOperator.

The retry policy is handled in default_args, and email alerts are configured.

Thus, this should meet all the constraints and the pipeline description.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='log_cleanup_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance'],
) as dag:

    start_task = EmptyOperator(task_id='start')

    cleanup_tasks = []
    for worker_id in range(3):
        cleanup_task = BashOperator(
            task_id=f'cleanup_worker_{worker_id}',
            bash_command="""
            lock_file="/tmp/airflow_log_cleanup_worker.lock"
            base_log_dir="{{ conf.get('logging', 'BASE_LOG_FOLDER') }}"
            max_age="{{ dag_run.conf.get('maxLogAgeInDays', var.value.get('airflow_log_cleanup__max_log_age_in_days', 7)) }}"
            enable_child_log="{{ var.value.get('airflow_log_cleanup__enable_delete_child_log', 'false') }}"
            
            if mkdir $lock_file; then
                trap 'rm -rf "$lock_file"' EXIT
                sleep $(({{ worker_id }} * 10))
                
                find "$base_log_dir" -type f -mtime "+$max_age" -delete
                
                if [ "$enable_child_log" = "true" ]; then
                    find "$base_log_dir" -mindepth 1 -type d -empty -delete
                fi
                
                find "$base_log_dir" -mindepth 1 -maxdepth 1 -type d -empty -delete
            else
                echo "Another cleanup process holds the lock"
                exit 0
            fi
            """,
        )
        cleanup_tasks.append(cleanup_task)

    start_task >> cleanup_tasks