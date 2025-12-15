from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import CronSchedule
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def start_backup_process():
    """Pipeline entry point."""
    pass

@task(cache_key_fn=task_input_hash)
def date_check_task():
    """Evaluates the execution date's day of week and returns the appropriate backup task."""
    today = datetime.now()
    if today.weekday() == 5:  # Saturday
        return 'full_backup_task'
    else:  # Weekday
        return 'incremental_backup_task'

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def full_backup_task():
    """Executes full database backup."""
    # Placeholder for full backup logic
    print("Executing full backup...")

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def incremental_backup_task():
    """Executes incremental database backup."""
    # Placeholder for incremental backup logic
    print("Executing incremental backup...")

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def verify_backup_task():
    """Validates backup integrity and completeness."""
    # Placeholder for backup verification logic
    print("Verifying backup...")

@task(cache_key_fn=task_input_hash)
def backup_complete():
    """Pipeline exit point."""
    pass

@flow
def database_backup_pipeline():
    """Orchestrates the database backup pipeline."""
    start_backup_process()

    backup_strategy = date_check_task()

    if backup_strategy == 'full_backup_task':
        backup_task = full_backup_task.submit()
    else:
        backup_task = incremental_backup_task.submit()

    verify_backup_task(wait_for=[backup_task])

    backup_complete()

if __name__ == '__main__':
    database_backup_pipeline()

# Deployment configuration (optional)
# DeploymentSpec(
#     name="daily-database-backup",
#     flow=database_backup_pipeline,
#     schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1), end_date=None),
#     flow_runner=SubprocessFlowRunner(),
#     parameters={},
#     tags=["backup"],
#     work_queue_name="default",
#     catchup=False,
# )