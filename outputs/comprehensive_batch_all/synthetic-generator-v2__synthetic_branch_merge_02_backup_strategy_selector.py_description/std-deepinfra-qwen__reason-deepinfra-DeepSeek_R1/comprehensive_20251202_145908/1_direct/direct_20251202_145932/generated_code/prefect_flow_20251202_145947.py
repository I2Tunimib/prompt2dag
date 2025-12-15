from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Completed, Failed
from prefect.futures import PrefectFuture
import os

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def start_backup_process():
    logger = get_run_logger()
    logger.info("Starting the backup process.")
    return True

@task(cache_key_fn=task_input_hash)
def date_check_task():
    logger = get_run_logger()
    today = datetime.now().weekday()  # 0=Monday, 6=Sunday
    if today == 5:  # Saturday
        logger.info("Executing full backup on Saturday.")
        return 'full_backup_task'
    else:
        logger.info("Executing incremental backup on weekdays.")
        return 'incremental_backup_task'

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def full_backup_task():
    logger = get_run_logger()
    logger.info("Executing full database backup.")
    # Simulate a full backup command
    os.system("echo 'Full backup completed'")
    return True

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def incremental_backup_task():
    logger = get_run_logger()
    logger.info("Executing incremental database backup.")
    # Simulate an incremental backup command
    os.system("echo 'Incremental backup completed'")
    return True

@task(cache_key_fn=task_input_hash, trigger=Completed, retries=2, retry_delay_seconds=300)
def verify_backup_task(full_backup_future: PrefectFuture, incremental_backup_future: PrefectFuture):
    logger = get_run_logger()
    logger.info("Verifying backup integrity and completeness.")
    if full_backup_future.state.is_completed() or incremental_backup_future.state.is_completed():
        logger.info("Backup verification successful.")
        return True
    else:
        logger.error("Backup verification failed.")
        raise Failed("Backup verification failed.")

@task(cache_key_fn=task_input_hash)
def backup_complete():
    logger = get_run_logger()
    logger.info("Backup process completed.")
    return True

@flow(name="Database Backup Pipeline", description="Automated database backup with conditional branching and verification.")
def database_backup_pipeline():
    start = start_backup_process()
    date_check = date_check_task()

    full_backup_future = full_backup_task.submit() if date_check.result() == 'full_backup_task' else None
    incremental_backup_future = incremental_backup_task.submit() if date_check.result() == 'incremental_backup_task' else None

    verify_backup = verify_backup_task(full_backup_future, incremental_backup_future)
    backup_complete()

if __name__ == '__main__':
    database_backup_pipeline()

# Deployment/Schedule Configuration (optional)
# Schedule: Daily execution via '@daily' interval
# Start Date: January 1, 2024
# Catchup: Disabled to prevent historical backfills
# Owner: Backup team
# Retry Policy: 2 retries with 5-minute delays between attempts