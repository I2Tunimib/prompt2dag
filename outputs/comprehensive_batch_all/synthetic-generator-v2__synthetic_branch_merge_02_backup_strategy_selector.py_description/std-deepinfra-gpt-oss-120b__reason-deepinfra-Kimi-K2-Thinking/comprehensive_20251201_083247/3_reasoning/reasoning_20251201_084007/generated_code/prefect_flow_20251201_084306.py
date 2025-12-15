from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime
import time

# Tasks
@task(retries=2, retry_delay_seconds=300)
def start_backup_process():
    """Pipeline entry point"""
    print("Starting backup process...")
    return "started"

@task(retries=2, retry_delay_seconds=300)
def date_check_task(run_date: str = None) -> str:
    """Determine backup strategy based on day of week"""
    if run_date:
        execution_date = datetime.fromisoformat(run_date)
    else:
        execution_date = datetime.now()
    
    day_of_week = execution_date.weekday()  # Monday=0, Sunday=6
    
    if day_of_week == 5:  # Saturday
        return "full_backup_task"
    else:  # Weekdays (Monday-Friday, Sunday)
        return "incremental_backup_task"

@task(retries=2, retry_delay_seconds=300)
def full_backup_task():
    """Execute full database backup (Saturday)"""
    print("Executing full database backup...")
    # Simulate backup work
    time.sleep(2)
    print("Full backup completed successfully")
    return "full_backup_complete"

@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task():
    """Execute incremental database backup (weekday)"""
    print("Executing incremental database backup...")
    # Simulate backup work
    time.sleep(2)
    print("Incremental backup completed successfully")
    return "incremental_backup_complete"

@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_result: str = None):
    """Validate backup integrity and completeness"""
    print(f"Verifying backup: {backup_result}")
    # Simulate verification
    time.sleep(1)
    print("Backup verification completed successfully")
    return "verification_complete"

@task(retries=2, retry_delay_seconds=300)
def backup_complete():
    """Pipeline exit point"""
    print("Backup process completed")
    return "completed"

# Flow
@flow(
    name="database-backup-pipeline",
    description="Automated database backup with branch-merge pattern",
    # Schedule can be configured during deployment:
    # schedule={"interval": 86400},  # Daily interval
    # or use Cron: schedule="0 0 * * *"
    task_runner=ConcurrentTaskRunner(),
)
def database_backup_flow(run_date: str = None):
    """
    Database backup pipeline with conditional branching
    
    Args:
        run_date: Optional ISO format date string for testing
    """
    # Start the pipeline
    start_result = start_backup_process()
    
    # Determine backup strategy
    backup_type = date_check_task(run_date)
    
    # Execute backup based on condition (parallel submission)
    # Only one will actually run based on the condition, but we submit both
    # to demonstrate the pattern
    full_result = full_backup_task.submit(wait_for=[start_result])
    incremental_result = incremental_backup_task.submit(wait_for=[start_result])
    
    # Wait for the appropriate backup based on condition
    if backup_type == "full_backup_task":
        backup_result = full_result.result()
        # Cancel the incremental task since it's not needed
        incremental_result.cancel()
    else:
        backup_result = incremental_result.result()
        # Cancel the full task since it's not needed
        full_result.cancel()
    
    # Verify backup with trigger rule: none_failed_min_one_success
    # In Prefect 2.x, we implement this logic manually
    # The task will run if at least one backup succeeded and none failed
    
    # Check states of backup tasks
    full_state = full_result.state
    incremental_state = incremental_result.state
    
    # Determine if we should run verification
    should_verify = False
    
    # If full backup succeeded
    if full_state.is_completed() and not full_state.is_failed():
        should_verify = True
        verification_input = "full_backup"
    # If incremental backup succeeded
    elif incremental_state.is_completed() and not incremental_state.is_failed():
        should_verify = True
        verification_input = "incremental_backup"
    else:
        # If both failed, raise an error
        raise RuntimeError("Both backup tasks failed. Verification cannot proceed.")
    
    if should_verify:
        verify_result = verify_backup_task(verification_input)
        # Complete the pipeline
        backup_complete(wait_for=[verify_result])
    else:
        raise RuntimeError("No successful backup to verify")

if __name__ == "__main__":
    # For local testing
    database_backup_flow()