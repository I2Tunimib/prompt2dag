from prefect import flow, task
from prefect.logging import get_run_logger
from datetime import datetime
import time
from typing import Any


@task
def start_backup_process() -> str:
    """Entry point for the backup pipeline."""
    logger = get_run_logger()
    logger.info("Starting backup process")
    return "started"


@task
def check_date_task() -> bool:
    """Determine if today is Saturday (full backup) or not (incremental)."""
    logger = get_run_logger()
    today = datetime.now()
    day_of_week = today.weekday()  # Monday=0, Sunday=6
    
    is_saturday = day_of_week == 5
    
    if is_saturday:
        logger.info("Saturday detected - executing full backup")
    else:
        logger.info(f"Weekday detected (day {day_of_week}) - executing incremental backup")
    
    return is_saturday


@task
def full_backup_task() -> str:
    """Execute full database backup (Saturday)."""
    logger = get_run_logger()
    logger.info("Executing full database backup")
    # Simulate backup work
    time.sleep(2)
    logger.info("Full backup completed successfully")
    return "full_backup"


@task
def incremental_backup_task() -> str:
    """Execute incremental database backup (weekday)."""
    logger = get_run_logger()
    logger.info("Executing incremental database backup")
    # Simulate backup work
    time.sleep(1)
    logger.info("Incremental backup completed successfully")
    return "incremental_backup"


@task
def verify_backup_task(backup_result: str) -> str:
    """Validate backup integrity and completeness."""
    logger = get_run_logger()
    logger.info(f"Verifying backup: {backup_result}")
    # Simulate verification
    time.sleep(1)
    logger.info("Backup verification completed successfully")
    return "verified"


@task
def backup_complete(verification_result: str) -> str:
    """Exit point for the backup pipeline."""
    logger = get_run_logger()
    logger.info("Backup process completed")
    return "completed"


@flow(
    name="database-backup-pipeline",
    retries=2,
    retry_delay_seconds=300,  # 5 minutes
    # Schedule: Daily execution starting Jan 1, 2024, catchup disabled
    # In Prefect 2.x, schedules are configured at deployment time
)
def database_backup_pipeline() -> None:
    """Orchestrates the database backup pipeline with conditional branching."""
    # Start the pipeline
    start_backup_process()
    
    # Determine backup type based on date
    is_saturday = check_date_task()
    
    # Execute appropriate backup task based on condition
    # Using .submit() to allow for potential parallel execution
    if is_saturday:
        backup_result = full_backup_task.submit()
    else:
        backup_result = incremental_backup_task.submit()
    
    # Wait for backup to complete and verify
    # The verification task will run regardless of which backup path was taken
    verification_result = verify_backup_task(backup_result)
    
    # Complete the pipeline
    backup_complete(verification_result)


if __name__ == "__main__":
    # For local execution
    database_backup_pipeline()