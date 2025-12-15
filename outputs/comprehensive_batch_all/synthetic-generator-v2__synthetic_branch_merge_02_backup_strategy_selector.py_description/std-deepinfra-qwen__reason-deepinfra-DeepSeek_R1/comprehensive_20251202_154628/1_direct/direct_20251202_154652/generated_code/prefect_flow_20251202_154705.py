from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def start_backup_process():
    """Pipeline entry point."""
    pass

@task(cache_key_fn=task_input_hash)
def date_check_task():
    """Evaluates the execution date's day of week and returns the appropriate backup task."""
    today = datetime.now().weekday()
    if today == 5:  # Saturday
        return 'full_backup_task'
    else:  # Weekdays
        return 'incremental_backup_task'

@task(cache_key_fn=task_input_hash)
def full_backup_task():
    """Executes full database backup."""
    # Placeholder for full backup logic
    print("Executing full database backup")

@task(cache_key_fn=task_input_hash)
def incremental_backup_task():
    """Executes incremental database backup."""
    # Placeholder for incremental backup logic
    print("Executing incremental database backup")

@task(cache_key_fn=task_input_hash, trigger="none_failed_min_one_success")
def verify_backup_task():
    """Validates backup integrity and completeness."""
    # Placeholder for backup verification logic
    print("Verifying backup integrity and completeness")

@task(cache_key_fn=task_input_hash)
def backup_complete():
    """Pipeline exit point."""
    pass

@flow(name="Database Backup Pipeline")
def database_backup_pipeline():
    """Orchestrates the database backup pipeline."""
    start_backup_process()

    backup_task = date_check_task()
    
    if backup_task.result() == 'full_backup_task':
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()

    verify_backup_task(wait_for=[backup_future])
    backup_complete()

if __name__ == '__main__':
    # Schedule: Daily execution via '@daily' interval
    # Start Date: January 1, 2024
    # Catchup: Disabled to prevent historical backfills
    # Owner: Backup team
    # Retry Policy: 2 retries with 5-minute delays between attempts
    # Deployment configuration
    DeploymentSpec(
        name="daily-backup",
        flow=database_backup_pipeline,
        schedule=IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1)),
        flow_runner=SubprocessFlowRunner(),
        catchup=False,
    )
    database_backup_pipeline()