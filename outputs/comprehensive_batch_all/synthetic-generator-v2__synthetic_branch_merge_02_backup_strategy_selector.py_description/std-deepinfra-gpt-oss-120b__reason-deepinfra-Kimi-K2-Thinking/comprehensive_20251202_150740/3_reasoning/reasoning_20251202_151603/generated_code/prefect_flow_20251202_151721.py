from prefect import flow, task
from datetime import datetime
import time


@task(retries=2, retry_delay_seconds=300)
def start_backup_process():
    """Entry point for the backup pipeline."""
    print("Starting backup process...")
    return "started"


@task(retries=2, retry_delay_seconds=300)
def date_check_task(execution_date: datetime):
    """Determine backup strategy based on day of week."""
    day_of_week = execution_date.weekday()
    if day_of_week == 5:
        return "full_backup_task"
    else:
        return "incremental_backup_task"


@task(retries=2, retry_delay_seconds=300)
def full_backup_task():
    """Execute full database backup (Saturday)."""
    print("Executing full database backup...")
    time.sleep(2)
    print("Full backup completed successfully")
    return "full_backup"


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task():
    """Execute incremental database backup (weekday)."""
    print("Executing incremental database backup...")
    time.sleep(1)
    print("Incremental backup completed successfully")
    return "incremental_backup"


@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_result):
    """Validate backup integrity and completeness."""
    print(f"Verifying backup: {backup_result}")
    time.sleep(1)
    print("Backup verification completed successfully")
    return "verified"


@task(retries=2, retry_delay_seconds=300)
def backup_complete():
    """Exit point for the backup pipeline."""
    print("Backup process complete")
    return "completed"


@flow(name="database-backup-pipeline")
def database_backup_pipeline(execution_date: datetime = None):
    """
    Database backup pipeline with conditional branching.
    
    Executes full backup on Saturdays and incremental on weekdays,
    then verifies the backup. Owner: Backup team.
    """
    if execution_date is None:
        execution_date = datetime.now()
    
    start_result = start_backup_process()
    branch = date_check_task(execution_date)
    
    if branch == "full_backup_task":
        backup_result = full_backup_task.submit()
    else:
        backup_result = incremental_backup_task.submit()
    
    verification = verify_backup_task(backup_result)
    complete_result = backup_complete()
    
    return complete_result


if __name__ == "__main__":
    # Schedule: Daily execution starting Jan 1, 2024, catchup disabled
    # In Prefect 2.x, schedule is configured at deployment time:
    # prefect deployment build backup_pipeline.py:database_backup_pipeline \
    #   --name daily-backup --cron "0 2 * * *" --apply
    database_backup_pipeline()