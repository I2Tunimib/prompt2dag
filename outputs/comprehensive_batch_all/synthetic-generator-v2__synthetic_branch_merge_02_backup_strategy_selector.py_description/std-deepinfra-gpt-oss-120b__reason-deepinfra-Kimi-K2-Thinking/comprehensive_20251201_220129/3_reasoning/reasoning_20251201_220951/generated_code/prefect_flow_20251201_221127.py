import pendulum
from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def start_backup_process():
    """Entry point for the backup pipeline."""
    print("Starting backup process...")
    return "backup_started"


@task(retries=2, retry_delay_seconds=300)
def date_check_task(run_date=None):
    """Determine backup type based on day of week."""
    if run_date is None:
        run_date = pendulum.now()
    
    day_of_week = run_date.day_of_week
    # 6 is Saturday in pendulum (0=Sunday, 6=Saturday)
    if day_of_week == 6:
        return "full_backup_task"
    else:
        return "incremental_backup_task"


@task(retries=2, retry_delay_seconds=300)
def full_backup_task():
    """Execute full database backup (Saturday)."""
    print("Executing full database backup...")
    return "full_backup_completed"


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task():
    """Execute incremental database backup (weekday)."""
    print("Executing incremental database backup...")
    return "incremental_backup_completed"


@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_result):
    """Validate backup integrity and completeness."""
    print(f"Verifying backup: {backup_result}")
    return "backup_verified"


@task(retries=2, retry_delay_seconds=300)
def backup_complete():
    """Exit point for the backup pipeline."""
    print("Backup process completed successfully.")
    return "backup_complete"


@flow(
    name="database-backup-pipeline",
    # Deployment: prefect deployment build backup_pipeline.py:backup_flow -n "daily-backup" --cron "0 2 * * *" --timezone "UTC" -a
    # Configure start_date=2024-01-01 and catchup=False in UI or via CLI
)
def backup_flow(run_date=None):
    """
    Database backup pipeline with conditional branching.
    
    Executes full backup on Saturdays and incremental backups on weekdays,
    then verifies the backup regardless of which path was taken.
    """
    start_future = start_backup_process.submit()
    
    backup_type_future = date_check_task.submit(run_date, wait_for=[start_future])
    backup_type = backup_type_future.result()
    
    if backup_type == "full_backup_task":
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()
    
    verification_future = verify_backup_task.submit(backup_future, wait_for=[backup_future])
    
    completion_future = backup_complete.submit(wait_for=[verification_future])
    
    return completion_future


if __name__ == "__main__":
    backup_flow()