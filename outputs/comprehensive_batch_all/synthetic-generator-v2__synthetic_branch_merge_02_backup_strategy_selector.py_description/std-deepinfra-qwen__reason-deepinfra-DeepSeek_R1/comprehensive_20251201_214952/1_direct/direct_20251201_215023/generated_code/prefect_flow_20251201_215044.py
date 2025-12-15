from datetime import datetime
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Completed, Failed
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner
from prefect.schedules import IntervalSchedule
from datetime import timedelta

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def start_backup_process():
    logger = get_run_logger()
    logger.info("Starting backup process.")
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

@task(cache_key_fn=task_input_hash)
def full_backup_task():
    logger = get_run_logger()
    logger.info("Executing full database backup.")
    # Simulate a full backup process
    return "Full backup completed."

@task(cache_key_fn=task_input_hash)
def incremental_backup_task():
    logger = get_run_logger()
    logger.info("Executing incremental database backup.")
    # Simulate an incremental backup process
    return "Incremental backup completed."

@task(cache_key_fn=task_input_hash, trigger=Completed)
def verify_backup_task(full_backup_result=None, incremental_backup_result=None):
    logger = get_run_logger()
    if full_backup_result:
        logger.info(f"Verifying full backup: {full_backup_result}")
    elif incremental_backup_result:
        logger.info(f"Verifying incremental backup: {incremental_backup_result}")
    else:
        logger.error("No backup results to verify.")
        raise Failed("No backup results to verify.")
    logger.info("Backup verification successful.")
    return "Backup verified."

@task(cache_key_fn=task_input_hash)
def backup_complete():
    logger = get_run_logger()
    logger.info("Backup process completed.")
    return True

@flow(name="Database Backup Pipeline")
def database_backup_pipeline():
    start = start_backup_process.submit()
    date_check = date_check_task.submit(start)
    
    full_backup_future = full_backup_task.submit() if date_check.result() == 'full_backup_task' else None
    incremental_backup_future = incremental_backup_task.submit() if date_check.result() == 'incremental_backup_task' else None
    
    verify_backup = verify_backup_task.submit(
        full_backup_result=full_backup_future,
        incremental_backup_result=incremental_backup_future
    )
    
    backup_complete.submit(verify_backup)

if __name__ == '__main__':
    # Schedule configuration (optional)
    # DeploymentSpec(
    #     name="daily-backup",
    #     flow=database_backup_pipeline,
    #     schedule=IntervalSchedule(interval=timedelta(days=1)),
    #     flow_runner=SubprocessFlowRunner(),
    #     start_date=datetime(2024, 1, 1),
    #     catchup=False
    # )
    database_backup_pipeline()