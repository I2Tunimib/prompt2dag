from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Failed
import os
import time
from pathlib import Path

# Constants
BASE_LOG_FOLDER = os.getenv("AIRFLOW__CORE__BASE_LOG_FOLDER", "/opt/airflow/logs")
LOCK_FILE_PATH = "/tmp/airflow_log_cleanup_worker.lock"
DEFAULT_MAX_LOG_AGE_IN_DAYS = int(os.getenv("AIRFLOW_LOG_CLEANUP__MAX_LOG_AGE_IN_DAYS", 30))
ENABLE_DELETE_CHILD_LOG = os.getenv("AIRFLOW_LOG_CLEANUP__ENABLE_DELETE_CHILD_LOG", "False").lower() == "true"

# Task definitions
@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def start_pipeline():
    logger = get_run_logger()
    logger.info("Pipeline started")
    return True

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def acquire_lock(lock_file_path):
    logger = get_run_logger()
    while True:
        try:
            with open(lock_file_path, "x") as lock_file:
                logger.info(f"Lock acquired: {lock_file_path}")
                return True
        except FileExistsError:
            logger.warning(f"Lock file exists: {lock_file_path}, waiting...")
            time.sleep(5)

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def release_lock(lock_file_path):
    logger = get_run_logger()
    try:
        os.remove(lock_file_path)
        logger.info(f"Lock released: {lock_file_path}")
    except FileNotFoundError:
        logger.warning(f"Lock file not found: {lock_file_path}")

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def delete_old_logs(log_directory, max_log_age_in_days):
    logger = get_run_logger()
    logger.info(f"Deleting old logs in {log_directory} with max age {max_log_age_in_days} days")
    find_command = f"find {log_directory} -type f -mtime +{max_log_age_in_days} -delete"
    os.system(find_command)
    return True

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def remove_empty_subdirectories(log_directory):
    logger = get_run_logger()
    logger.info(f"Removing empty subdirectories in {log_directory}")
    find_command = f"find {log_directory} -type d -empty -delete"
    os.system(find_command)
    return True

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def remove_empty_top_level_directories(log_directory):
    logger = get_run_logger()
    logger.info(f"Removing empty top-level directories in {log_directory}")
    find_command = f"find {log_directory} -mindepth 1 -type d -empty -delete"
    os.system(find_command)
    return True

@task(retries=1, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def cleanup_worker(log_directory, max_log_age_in_days, sleep_time=0):
    logger = get_run_logger()
    logger.info(f"Starting cleanup for {log_directory} with max age {max_log_age_in_days} days")
    time.sleep(sleep_time)  # Stagger worker execution
    try:
        acquire_lock(LOCK_FILE_PATH)
        delete_old_logs(log_directory, max_log_age_in_days)
        remove_empty_subdirectories(log_directory)
        remove_empty_top_level_directories(log_directory)
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise Failed(message=f"Error during cleanup: {e}")
    finally:
        release_lock(LOCK_FILE_PATH)
    return True

# Flow definition
@flow(name="Airflow Log Cleanup Pipeline")
def log_cleanup_pipeline(max_log_age_in_days=DEFAULT_MAX_LOG_AGE_IN_DAYS):
    logger = get_run_logger()
    start_pipeline()
    log_directories = [str(d) for d in Path(BASE_LOG_FOLDER).iterdir() if d.is_dir()]
    if ENABLE_DELETE_CHILD_LOG:
        log_directories.append(os.getenv("AIRFLOW__CORE__SCHEDULER_CHILD_PROCESS_LOG_DIRECTORY", "/opt/airflow/scheduler_logs"))

    # Parallel log cleanup tasks
    cleanup_tasks = []
    for i, log_directory in enumerate(log_directories):
        sleep_time = i * 5  # Stagger workers by 5 seconds
        cleanup_tasks.append(cleanup_worker.submit(log_directory, max_log_age_in_days, sleep_time))

    # Wait for all cleanup tasks to complete
    for task in cleanup_tasks:
        task.result()

if __name__ == "__main__":
    log_cleanup_pipeline()

# Optional: Deployment/schedule configuration
# Schedule: Daily execution via @daily interval
# Start date: One day prior to current date
# Catchup disabled to prevent backfill
# Retry policy: 1 retry with 1-minute delay
# Email alerts configured for failure conditions
# Paused on creation: False (DAG starts active)