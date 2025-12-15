from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.deployments import DeploymentSpec
from prefect.filesystems import LocalFileSystem
from prefect.context import get_run_context
import os
import time
import shutil

# Constants
BASE_LOG_FOLDER = os.getenv("AIRFLOW__CORE__BASE_LOG_FOLDER", "/opt/airflow/logs")
LOCK_FILE = "/tmp/airflow_log_cleanup_worker.lock"
MAX_LOG_AGE_IN_DAYS = int(os.getenv("AIRFLOW_LOG_CLEANUP__MAX_LOG_AGE_IN_DAYS", 30))
ENABLE_DELETE_CHILD_LOG = os.getenv("AIRFLOW_LOG_CLEANUP__ENABLE_DELETE_CHILD_LOG", "False").lower() == "true"

# Task definitions
@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def start_pipeline():
    logger = get_run_logger()
    logger.info("Pipeline started")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def acquire_lock(lock_file):
    logger = get_run_logger()
    while os.path.exists(lock_file):
        logger.warning(f"Lock file {lock_file} exists. Waiting for 10 seconds...")
        time.sleep(10)
    with open(lock_file, "w") as f:
        f.write("1")
    logger.info(f"Lock acquired: {lock_file}")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def release_lock(lock_file):
    logger = get_run_logger()
    if os.path.exists(lock_file):
        os.remove(lock_file)
        logger.info(f"Lock released: {lock_file}")
    else:
        logger.warning(f"Lock file {lock_file} does not exist")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def delete_old_logs(log_dir, max_log_age_in_days):
    logger = get_run_logger()
    logger.info(f"Deleting old logs in {log_dir} with max age {max_log_age_in_days} days")
    os.system(f"find {log_dir} -type f -mtime +{max_log_age_in_days} -delete")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def remove_empty_subdirectories(log_dir):
    logger = get_run_logger()
    logger.info(f"Removing empty subdirectories in {log_dir}")
    for root, dirs, _ in os.walk(log_dir, topdown=False):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logger.info(f"Removed empty directory: {dir_path}")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def remove_empty_top_level_directories(log_dir):
    logger = get_run_logger()
    logger.info(f"Removing empty top-level directories in {log_dir}")
    if not os.listdir(log_dir):
        os.rmdir(log_dir)
        logger.info(f"Removed empty top-level directory: {log_dir}")
    return True

@task(cache_key_fn=task_input_hash, retries=1, retry_delay_seconds=60)
def cleanup_worker(log_dir, max_log_age_in_days, enable_delete_child_log):
    logger = get_run_logger()
    logger.info(f"Starting cleanup for {log_dir}")
    try:
        delete_old_logs(log_dir, max_log_age_in_days)
        remove_empty_subdirectories(log_dir)
        if enable_delete_child_log:
            remove_empty_top_level_directories(log_dir)
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        release_lock(LOCK_FILE)
    return True

# Flow definition
@flow(name="Airflow Log Cleanup", schedule=IntervalSchedule(interval=timedelta(days=1)))
def airflow_log_cleanup_flow():
    logger = get_run_logger()
    start_pipeline()
    max_log_age_in_days = int(get_run_context().parameters.get("maxLogAgeInDays", MAX_LOG_AGE_IN_DAYS))
    log_dirs = [os.path.join(BASE_LOG_FOLDER, d) for d in os.listdir(BASE_LOG_FOLDER) if os.path.isdir(os.path.join(BASE_LOG_FOLDER, d))]
    
    # Acquire lock
    acquire_lock(LOCK_FILE)
    
    # Parallel cleanup tasks
    cleanup_tasks = [cleanup_worker.submit(log_dir, max_log_age_in_days, ENABLE_DELETE_CHILD_LOG) for log_dir in log_dirs]
    
    # Wait for all cleanup tasks to complete
    for task in cleanup_tasks:
        task.result()

if __name__ == "__main__":
    airflow_log_cleanup_flow()