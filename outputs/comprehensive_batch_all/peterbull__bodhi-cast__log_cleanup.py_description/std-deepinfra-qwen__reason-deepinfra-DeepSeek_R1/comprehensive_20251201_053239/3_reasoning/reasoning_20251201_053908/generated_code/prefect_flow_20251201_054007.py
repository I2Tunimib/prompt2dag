<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements.

First, the pipeline is for cleaning up old Airflow log files. The main steps involve starting the flow, running parallel cleanup tasks, each handling log directories with locking, and then some three-phase cleanup process. The original uses Airflow's BashOperator and EmptyOperator, but in Prefect, I'll use tasks and flows.

Constraints mention using @task and @flow decorators. For parallel steps, I should use .submit(), which runs tasks concurrently. The flow should orchestrate the tasks in order.

The pipeline starts with an EmptyOperator equivalent, which in Prefect could be a simple task that does nothing. Then, the parallel log cleanup tasks. Each of these tasks needs to execute bash commands with file-based locking. Since these are parallel, each task instance should be submitted using .submit().

The three-phase cleanup involves deleting old logs, removing empty subdirs, and then top-level dirs. Each of these could be separate tasks, but the description says it's a three-phase process within each parallel task. So maybe each parallel task calls a function that does all three steps, or perhaps each step is a task. But since they are part of the same parallel worker, maybe it's better to have a single task that does all three steps, using subprocess or something.

Wait, the original uses BashOperator, which runs a bash command. So in Prefect tasks, I can use subprocess.run or similar to execute shell commands.

Each worker task needs to use a lock file. So perhaps using a context manager that creates the lock file, and ensures it's removed even on failure.

Configuration retrieval: The original uses Airflow variables and DAG run parameters. In Prefect, we can use parameters passed to the flow, or maybe Prefect configurations. But the user wants to have DAG run parameters for runtime override, so maybe the flow has a parameter max_log_age_in_days which can be overridden, and other variables like enable_delete_child_log can be parameters as well.

The flow should be scheduled daily, but Prefect handles schedules through deployments. The user mentioned to include a comment about deployment/schedule if schedules are mentioned. So maybe add a comment at the end about setting up a deployment with a daily schedule.

Retry policy: Prefect tasks can have retries. So each task can have retries=1 and retry_delay=60 (seconds).

Email alerts: Prefect has notifications, but that's usually handled via deployments or workflows. Since the code is supposed to be self-contained, maybe just a comment indicating that email alerts would be configured in the deployment settings.

Now, structuring the code:

- Start task: a @task that does nothing, maybe just logs a message.
- Then, multiple parallel cleanup tasks. Since the number of workers isn't specified, perhaps the flow takes a parameter for the number of workers, or the directories to process. But the original mentions "each worker processes one or more log directories". So maybe the flow gets a list of log directories, perhaps from the BASE_LOG_FOLDER and possibly the scheduler child log dir. But how to get that? The original uses Airflow's BASE_LOG_FOLDER, which here could be an environment variable or a parameter.

Wait, the problem says to use minimal safe defaults. So perhaps the flow has a parameter for base_log_folder, defaulting to an environment variable or a default path.

The parallel tasks would each handle a directory. So the flow could split the list of directories into workers, perhaps each worker handles a subset. But the original says "parallel cleanup processes across multiple worker nodes", which might imply that each task is responsible for a directory. Alternatively, each task could process all directories but with locking. Hmm, maybe each task is responsible for a specific directory, and they run in parallel.

But the problem says "parallel log cleanup tasks: Multiple BashOperator tasks execute in parallel fan-out pattern". So fan-out: perhaps the flow generates multiple tasks, each handling a different log directory.

So first, the flow needs to collect all the log directories to process. That could be from BASE_LOG_FOLDER and optionally the scheduler child log dir if enabled.

So steps:

1. Start task (trivial)
2. Get log directories to process (maybe a task that retrieves the base log folder and optional child log dir)
3. For each directory, run a cleanup task in parallel.

But how to structure this? Let's outline:

@flow
def airflow_log_cleanup_flow(...):
    start_task()
    log_dirs = get_log_directories_task()
    for dir in log_dirs:
        cleanup_task.submit(dir, ...)

But wait, the original uses multiple BashOperators in parallel. Each of those would be a separate task in Prefect, submitted with .submit().

So the get_log_directories_task would return a list of directories to process. Then, for each directory, submit a cleanup task.

Each cleanup task would handle the three-phase cleanup for that directory, using locking.

So the tasks would be:

- start_task: just a placeholder.
- get_log_directories: retrieves the list of directories to process.
- log_cleanup_worker: the task that does the actual cleanup for a directory, with locking.

Now, the log_cleanup_worker task needs to:

- Use a lock file in /tmp/airflow_log_cleanup_worker.lock (but perhaps per directory? Or per worker? The original says "file-based locking to prevent concurrent execution conflicts". Maybe each worker uses a lock file, but if multiple workers are running, each would have their own lock? Or perhaps the lock is per directory. The original description says "file-based locking using /tmp/airflow_log_cleanup_worker.lock". Wait, the lock file name is static, which suggests that only one worker can run at a time. But the original says the tasks run in parallel. That seems conflicting. Maybe the lock is per worker, but the name is fixed. That would mean that only one worker can run at a time, which contradicts the parallel execution. Hmm, perhaps the lock file name includes the directory or some unique identifier. Wait, the original description says "file-based locking using /tmp/airflow_log_cleanup_worker.lock". So same lock file for all workers? That would prevent any parallelism. That doesn't make sense. Maybe the lock file is per worker, but the description is ambiguous. Perhaps the lock file is per directory. For example, for directory /logs/dir1, the lock file is /tmp/airflow_log_cleanup_worker_dir1.lock. But the original description doesn't specify that. This is a problem. Since the user's pipeline uses a single lock file, that would serialize all workers, but the original says they run in parallel. That's a contradiction. Maybe the lock is per worker process, but the problem description is unclear. Alternatively, perhaps the lock is to prevent multiple runs of the entire DAG, but the original says the tasks run in parallel. Hmm. Maybe the lock is per worker task instance. But how?

Alternatively, perhaps the lock is a global lock to prevent multiple instances of the same DAG from running concurrently. But the original says the tasks implement file-based locking. So each task (BashOperator) would create a lock file. If all tasks use the same lock file, then they can't run in parallel. So perhaps the lock file is per task or per directory. Since the problem statement isn't clear, I need to make an assumption. Let's assume that each worker task uses a unique lock file name, perhaps based on the directory it's processing. For example, if the directory is /path/to/logs, the lock file could be /tmp/airflow_log_cleanup_worker_<hash_of_directory>.lock. Alternatively, maybe the lock file is created in the directory itself. But the problem says the lock file is in /tmp. So perhaps the lock file name includes the directory path. But since directory paths can have slashes, maybe replace slashes with underscores. For example, /tmp/airflow_log_cleanup_worker_<dir_path>.lock. But this is speculative. Alternatively, the original code may have used a single lock file, which would serialize all tasks, but the user says the tasks run in parallel. That's conflicting. To resolve this, perhaps the lock is per worker task, but the problem statement's description is incorrect. Alternatively, maybe the lock is to ensure that each worker doesn't run the same directory concurrently. But without more info, perhaps the lock file is per worker, but the code uses a unique identifier. Alternatively, perhaps the lock is not per directory but per task instance. But this is unclear.

Given the ambiguity, perhaps the code should create a lock file with a unique name per task. But how? Since the tasks are submitted in parallel, each would generate their own lock file. Wait, but the original uses BashOperator tasks that each run a script with the lock file. If all tasks use the same lock file, then they would wait for each other, effectively serializing. But the user says they run in parallel. Therefore, perhaps the lock file is not per task but per something else. Alternatively, maybe the lock is to prevent multiple instances of the same task from running on the same directory. But again, unclear.

Given the ambiguity, perhaps the code should use a lock file per directory. So for each directory being processed, the task creates a lock file named after that directory. For example, if the directory is /var/log/airflow, the lock file could be /tmp/airflow_log_cleanup_worker_var_log_airflow.lock. This way, multiple tasks can run in parallel as long as they are processing different directories, but if two tasks are for the same directory, they would wait. However, the original description says "parallel cleanup processes across multiple worker nodes", which suggests that each worker handles a directory, and multiple can run in parallel. So the lock is per directory to prevent concurrent processing of the same directory. That makes sense.

So, the code for the log_cleanup_worker task would, for a given directory, create a lock file specific to that directory. The task would then proceed only if it can acquire the lock.

But implementing file-based locking in Python requires handling the creation and deletion of the lock file, even in case of failures. So using a try/finally block or a context manager.

Now, the three-phase cleanup:

1. Delete old log files using find with mtime.
2. Remove empty subdirectories.
3. Remove empty top-level directories.

Each of these can be done via subprocess calls.

The configuration parameters:

- max_log_age_in_days: from Airflow variables or DAG parameters. In Prefect, this can be a flow parameter with a default from an environment variable or a default value.
- enable_delete_child_log: boolean to include the scheduler child process log directory.

The flow should retrieve the list of log directories based on BASE_LOG_FOLDER and optionally the scheduler child log dir.

So, the get_log_directories task would:

- Get the base_log_folder (from configuration, defaulting to a path)
- If enable_delete_child_log is True, add the scheduler child log dir (maybe from another variable)
- Return the list of directories to process.

But how to get these values? The original uses Airflow variables, but in Prefect, these can be passed as parameters to the flow or retrieved from Prefect settings or environment variables.

Assuming the flow has parameters for max_log_age_in_days, enable_delete_child_log, base_log_folder, etc.

Putting it all together:

The flow would start with a start task (maybe just a no-op task), then get the log directories, then for each directory, submit a cleanup worker task in parallel.

Each cleanup worker task would:

- Use a lock file based on the directory.
- Execute the three cleanup steps.
- Handle errors and ensure lock file is removed.

Now, implementing the lock as a context manager:

class FileLock:
    def __init__(self, lock_path):
        self.lock_path = lock_path

    def __enter__(self):
        # Try to create the lock file
        # Use O_EXCL to prevent race conditions
        import os
        fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        self.fd = fd
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import os
        os.close(self.fd)
        os.remove(self.lock_path)
        return False

But this may not be completely atomic. Alternatively, use fcntl or another method. However, for simplicity, perhaps use a try-except block around creating the file.

But implementing a reliable file lock in Python can be tricky. Alternatively, use the fasteners library, but that requires an external dependency. Since the user wants minimal code, perhaps implement a simple file-based lock with a check.

But the problem says to make the code executable assuming required packages are installed. So if using fasteners, need to include it, but the user didn't mention it. So perhaps stick to basic file operations.

Alternatively, use a directory-based approach where creating a directory is atomic. But again, perhaps better to proceed with a simple lock file.

So, in the log_cleanup_worker task:

def log_cleanup_worker(log_dir: str, max_log_age: int, enable_child_log: bool):
    lock_file = f"/tmp/airflow_log_cleanup_worker_{log_dir.replace('/', '_')}.lock"
    try:
        # Acquire lock
        if os.path.exists(lock_file):
            raise Exception("Lock file exists, skipping")
        with open(lock_file, 'w') as f:
            f.write("")
        # Perform cleanup steps
        # Step 1: Delete old log files
        subprocess.run(f"find {log_dir} -type f -mtime +{max_log_age} -delete", shell=True, check=True)
        # Step 2: Remove empty subdirectories
        subprocess.run(f"find {log_dir} -mindepth 1 -type d -empty -delete", shell=True, check=True)
        # Step 3: Remove empty top-level directories
        subprocess.run(f"find {log_dir} -mindepth 0 -type d -empty -delete", shell=True, check=True)
    except Exception as e:
        # Handle error, maybe log it
        raise
    finally:
        # Release lock
        if os.path.exists(lock_file):
            os.remove(lock_file)

But this isn't atomic. Another process could create the lock file after the check but before creating it. To make it atomic, we can use os.open with O_EXCL:

import os

def acquire_lock(lock_path):
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False

Then, in the task:

lock_acquired = acquire_lock(lock_file)
if not lock_acquired:
    # Skip processing, log a message
    return
try:
    # perform steps
finally:
    os.remove(lock_file)

This way, the lock is atomic.

So integrating this into the task.

Now, error handling: the original mentions graceful error handling with lock file cleanup on failures. So the finally block ensures the lock is removed even if an error occurs.

Now, the configuration retrieval:

The flow can have parameters like max_log_age_in_days, which can be overridden. The default can come from an Airflow variable, but in Prefect, perhaps use a default parameter value, or retrieve from environment variables.

But the original mentions that the DAG run configuration can override the maxLogAgeInDays parameter. So in Prefect, the flow can take a parameter max_log_age_in_days, which can be set when calling the flow. The default can be from an environment variable or a default value.

Similarly, enable_delete_child_log can be a flow parameter.

Putting it all together:

The flow function would have parameters for these configurations.

Now, scheduling: the original mentions daily execution. In Prefect, this is handled via deployments. So the code can include a comment indicating that a deployment with a daily schedule should be created.

Retry policy: each task can have retries=1 and retry_delay=60 (seconds). So in the @task decorator, set retries=1, retry_delay_seconds=60.

Email alerts: Prefect has notification mechanisms, but they are typically configured via the UI or deployment YAML. Since the code should be self-contained, perhaps add a comment indicating that email alerts are configured in the deployment settings.

Putting all this into code:

Start by importing necessary modules: prefect, os, subprocess, etc.

Then define the tasks:

- start_pipeline: a task that logs a message.
- get_log_directories: retrieves the list of directories based on base_log_folder and enable_child_log.
- log_cleanup_worker: performs the cleanup for a directory, with locking and retries.

The flow function orchestrates these tasks.

Now, code:

from prefect import flow, task
from typing import List
import os
import subprocess
import time

@task(retries=1, retry_delay_seconds=60)
def start_pipeline():
    """Initiates the cleanup pipeline."""
    print("Starting Airflow log cleanup pipeline")

@task
def get_log_directories(base_log_folder: str, enable_child_log: bool) -> List[str]:
    """Retrieve list of log directories to process."""
    log_dirs = [base_log_folder]
    if enable_child_log:
        # Assuming child log directory is under base_log_folder or from another variable
        child_log_dir = os.path.join(base_log_folder, "scheduler_child_logs")
        log_dirs.append(child_log_dir)
    return log_dirs

@task(retries=1, retry_delay_seconds=60)
def log_cleanup_worker(log_dir: str, max_log_age: int):
    """Clean up old logs in specified directory with file-based locking."""
    safe_dir = log_dir.replace("/", "_").replace(" ", "_")
    lock_file = f"/tmp/airflow_log_cleanup_worker_{safe_dir}.lock"
    
    try:
        # Attempt to acquire lock
        if not acquire_lock(lock_file):
            print(f"Could not acquire lock for {log_dir}, skipping")
            return
        print(f"Acquired lock for {log_dir}")
        
        # Phase 1: Delete old log files
        subprocess.run(
            f"find {log_dir} -type f -mtime +{max_log_age} -delete",
            shell=True,
            check=True
        )
        
        # Phase 2: Remove empty subdirectories
        subprocess.run(
            f"find {log_dir} -mindepth 1 -type d -empty -delete",
            shell=True,
            check=True
        )
        
        # Phase 3: Remove empty top-level directories
        subprocess.run(
            f"find {log_dir} -mindepth 0 -type d -empty -delete",
            shell=True,
            check=True
        )
        
        print(f"Cleanup completed for {log_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error during cleanup of {log_dir}: {e}")
        raise
    finally:
        release_lock(lock_file)

def acquire_lock(lock_path: str) -> bool:
    """Atomically acquire a file lock."""
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False

def release_lock(lock_path: str):
    """Release file lock by removing lock file."""
    try:
        os.remove(lock_path)
    except OSError:
        pass

@flow(
    name="airflow_log_cleanup",
    description="Automated cleanup of old Airflow log files."
)
def airflow_log_cleanup_flow(
    base_log_folder: str = os.getenv("AIRFLOW_BASE_LOG_FOLDER", "/opt/airflow/logs"),
    max_log_age_in_days: int = os.getenv("AIRFLOW_LOG_CLEANUP__MAX_LOG_AGE_DAYS", 30),
    enable_delete_child_log: bool = False,
    worker_sleep_time: int = 5
):
    start_pipeline()
    log_dirs = get_log_directories(base_log_folder, enable_delete_child_log)
    
    for dir_path in log_dirs:
        # Submit workers with staggered start to distribute load
        time.sleep(worker_sleep_time)
        log_cleanup_worker.submit(dir_path, max_log_age_in_days)

if __name__ == "__main__":
    # Example: Local execution with default parameters
    airflow_log_cleanup_flow()

# To deploy with a daily schedule:
# prefect deployment build ./module.py:airflow_log_cleanup_flow \
#     --name "Airflow Log Cleanup" \
#     --schedule "0 0 * * *" \
#     --tag maintenance \
#     --apply

Wait, but the worker_sleep_time is used to stagger the submission of tasks. However, when using .submit(), each task is submitted asynchronously. The sleep here would stagger the submission, not the execution. But perhaps the original intention is to stagger the start of each worker to distribute load. However, in Prefect, when you call .submit(), the task is sent to the task runner for execution. If using a concurrent task runner like Dask, the tasks would run in parallel. The sleep between submissions might not have the intended effect. Alternatively, perhaps the sleep is inside the worker task. But the original says "configurable sleep time staggers worker execution to distribute load". So maybe each worker task sleeps for a certain time before starting. But how to stagger them? Perhaps each worker task has a random or incremental sleep. But the user says "configurable sleep time", so perhaps the flow has a parameter worker_sleep_time which is multiplied by the index of the directory in the list. For example, for the first directory, sleep 0 seconds, second sleep worker_sleep_time, etc. But the code as written sleeps worker_sleep_time between each submission. However, when using .submit(), the tasks are submitted immediately, but the sleep would delay the submission loop. So if there are three directories, the first task is submitted, then sleep for 5 seconds, then submit the next, etc. This would stagger the submission, but depending on the task runner, they might start executing immediately. This might not effectively stagger the execution. Alternatively, the sleep could be inside the worker task. For example, each worker task sleeps for a certain amount based on its order. But how to pass that? Maybe the flow passes a sleep time to each worker, incrementing by the index. But the code currently doesn't do that. Hmm. The original says "configurable sleep time staggers worker execution to distribute load". So perhaps each worker sleeps for a certain time before starting. The sleep time could be a parameter passed to the worker task. For example, each worker task could have a sleep time that's multiplied by its index. But how to track the index? Since the worker tasks are submitted in a loop, perhaps pass a delay parameter based on the loop index. For example:

for i, dir_path in enumerate(log_dirs):
    log_cleanup_worker.submit(dir_path, max_log_age_in_days, sleep_time=i * worker_sleep_time)

Then, in the worker task:

@task(...)
def log_cleanup_worker(log_dir: str, max_log_age: int, sleep_time: int):
    time.sleep(sleep_time)
    # rest of the code

But this would stagger each worker's start time by sleep_time seconds. However, the original description says "configurable sleep time staggers worker execution", which suggests that each worker sleeps for a certain time before starting. So adding this to the task.

But the original code didn't mention this, but the pipeline description includes it as part of the steps. So need to include it.

So modifying the log_cleanup_worker task to include a sleep_time parameter, which is multiplied by the index. So in the flow:

for i, dir_path in enumerate(log_dirs):
    log_cleanup_worker.submit(
        log_dir=dir_path,
        max_log_age=max_log_age_in_days,
        sleep_time=i * worker_sleep_time
    )

And in the task:

@task(...)
def log_cleanup_worker(log_dir: str, max_log_age: int, sleep_time: int):
    time.sleep(sleep_time)
    # rest of the code

But the original description says "configurable sleep time staggers worker execution to distribute load". So the sleep_time is a parameter, perhaps passed from the flow. So the flow has a parameter worker_sleep_time, which defaults to 5 seconds, and each worker task sleeps for that amount multiplied by their index.

This way, the first worker starts immediately (sleep_time=0), the next after 5 seconds, etc. This staggers their execution.

So integrating this into the code.

But in the current code draft, the worker_sleep_time is passed as a flow parameter, and in the loop, each task is submitted with sleep_time=i * worker_sleep_time.

So updating the log_cleanup_worker task to include the sleep_time parameter and sleep accordingly.

Thus, the final code would have:

In the flow:

for i, dir_path in enumerate(log_dirs):
    log_cleanup_worker.submit(
        dir_path,
        max_log_age_in_days,
        sleep_time=i * worker_sleep_time
    )

In the task:

@task(retries=1, retry_delay_seconds=60)
def log_cleanup_worker(log_dir: str, max_log_age: int, sleep_time: int):
    time.sleep(sleep_time)
    # rest of the code

But wait, the sleep_time is passed per task. So each task will sleep for their respective time before proceeding. This staggers their execution start times.

This should distribute the load.

Now, putting all together:

The flow function has parameters for base_log_folder, max_log_age_in_days, enable_delete_child_log, worker_sleep_time.

The get_log_directories task returns the list of directories.

Each directory is processed by a log_cleanup_worker task, submitted with .submit(), passing sleep_time=i * worker_sleep_time.

The log_cleanup_worker task sleeps for sleep_time, then proceeds with locking and cleanup.

Now, testing for PEP 8 compliance: function names are lowercase_with_underscores, parameters same, etc.

The code should include all necessary imports, like time.

Wait, the log_cleanup_worker uses time.sleep, so need to import time.

Yes, in the code draft, 'import time' is present.

Now, error handling: the log_cleanup_worker has retries=1. If a task fails, it retries once after 60 seconds.

The lock file is handled in the task: if the task fails and retries, the lock file would have been released in the finally block during the first attempt. So the retry would attempt to acquire the lock again.

But if the task fails during the first attempt, the lock is released, so the retry can proceed.

Now, the email alerts: the original mentions email alerts on failure. Prefect allows notifications on state changes. To configure this, one would use automation in the UI or in a deployment YAML. Since the code can't include that, a comment is added indicating that email alerts are configured via deployment settings.

The schedule comment is added at the end as a comment, showing how to deploy with a daily schedule.

The code includes the if __name__ == '__main__': guard to run the flow locally.

Potential issues:

- The BASE_LOG_FOLDER is retrieved from an environment variable, defaulting to /opt/airflow/logs. This is Airflow's default, but the user should set it as needed.

- The max_log_age_in_days is retrieved from an environment variable, defaulting to 30.

- The enable_delete_child_log is a boolean parameter, defaulting to False.

- The worker_sleep_time is a flow parameter, defaulting to 5 seconds.

- The lock file name is based on the directory path, replacing '/' with '_' to avoid issues.

- The subprocess.run calls use shell=True, which could be a security risk if the log_dir is not trusted. But assuming the flow is run in a controlled environment, this is acceptable.

- The find commands may vary between systems. For example, some versions of find may not have -delete. But assuming a compatible system.

- The code uses print statements, but in Prefect, it's better to use the logger. However, the user didn't specify, and print is acceptable for simplicity.

Thus, the final code should look like the above.
</think>

from prefect import flow, task
from typing import List
import os
import subprocess
import time

@task(retries=1, retry_delay_seconds=60)
def start_pipeline():
    """Initiates the cleanup pipeline."""
    print("Starting Airflow log cleanup pipeline")

@task
def get_log_directories(base_log_folder: str, enable_child_log: bool) -> List[str]:
    """Retrieve list of log directories to process."""
    log_dirs = [base_log_folder]
    if enable_child_log:
        child_log_dir = os.path.join(base_log_folder, "scheduler_child_logs")
        log_dirs.append(child_log_dir)
    return log_dirs

def acquire_lock(lock_path: str) -> bool:
    """Atomically acquire a file lock."""
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False

def release_lock(lock_path: str):
    """Release file lock by removing lock file."""
    try:
        os.remove(lock_path)
    except OSError:
        pass

@task(retries=1, retry_delay_seconds=60)
def log_cleanup_worker(log_dir: str, max_log_age: int, sleep_time: int):
    """Clean up old logs in specified directory with file-based locking."""
    time.sleep(sleep_time)
    safe_dir = log_dir.replace("/", "_").replace(" ", "_")
    lock_file = f"/tmp/airflow_log_cleanup_worker_{safe_dir}.lock"
    
    try:
        if not acquire_lock(lock_file):
            print(f"Could not acquire lock for {log_dir}, skipping")
            return
        
        print(f"Processing {log_dir}")
        subprocess.run(
            f"find {log_dir} -type f -mtime +{max_log_age} -delete",
            shell=True,
            check=True
        )
        subprocess.run(
            f"find {log_dir} -mindepth 1 -type d -empty -delete",
            shell=True,
            check=True
        )
        subprocess.run(
            f"find {log_dir} -mindepth 0 -type d -empty -delete",
            shell=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error processing {log_dir}: {str(e)}")
        raise
    finally:
        release_lock(lock_file)

@flow(
    name="airflow_log_cleanup",
    description="Automated cleanup of old Airflow log files."
)
def airflow_log_cleanup_flow(
    base_log_folder: str = os.getenv("AIRFLOW_BASE_LOG_FOLDER", "/opt/airflow/logs"),
    max_log_age_in_days: int = int(os.getenv("AIRFLOW_LOG_CLEANUP__MAX_LOG_AGE_DAYS", 30)),
    enable_delete_child_log: bool = False,
    worker_sleep_stagger: int = 5
):
    start_pipeline()
    log_dirs = get_log_directories(base_log_folder, enable_delete_child_log)
    
    for idx, dir_path in enumerate(log_dirs):
        log_cleanup_worker.submit(
            dir_path,
            max_log_age_in_days,
            idx * worker_sleep_stagger
        )

if __name__ == "__main__":
    airflow_log_cleanup_flow()

# Deployment configuration example:
# prefect deployment build airflow_log_cleanup.py:airflow_log_cleanup_flow \
#     --name "Log Cleanup" \
#     --schedule "0 0 * * *" \
#     --tag maintenance \
#     --apply