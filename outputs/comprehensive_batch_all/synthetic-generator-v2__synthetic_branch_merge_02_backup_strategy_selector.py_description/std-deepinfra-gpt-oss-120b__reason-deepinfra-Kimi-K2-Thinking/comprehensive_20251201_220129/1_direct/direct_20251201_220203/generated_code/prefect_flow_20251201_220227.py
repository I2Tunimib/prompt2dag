from datetime import datetime
from prefect import flow, task, get_run_logger


@task
def start_backup_process() -> None:
    """Entry point for the backup pipeline."""
    logger = get_run_logger()
    logger.info("Starting backup process")


@task
def date_check_task() -> str:
    """
    Determine backup strategy based on the current UTC day of week.

    Returns:
        str: ``'full'`` for Saturday, otherwise ``'incremental'``.
    """
    logger = get_run_logger()
    now = datetime.utcnow()
    day_of_week = now.weekday()  # Monday=0, Saturday=5, Sunday=6
    logger.info("Current UTC day of week: %s", day_of_week)
    if day_of_week == 5:  # Saturday
        logger.info("Selected full backup strategy")
        return "full"
    logger.info("Selected incremental backup strategy")
    return "incremental"


@task(retries=2, retry_delay_seconds=300)
def full_backup_task() -> str:
    """Execute a full database backup."""
    logger = get_run_logger()
    logger.info("Running full database backup")
    # Placeholder for actual backup command, e.g., subprocess.run([...])
    return "full_backup_success"


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task() -> str:
    """Execute an incremental database backup."""
    logger = get_run_logger()
    logger.info("Running incremental database backup")
    # Placeholder for actual backup command, e.g., subprocess.run([...])
    return "incremental_backup_success"


@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_result: str) -> str:
    """
    Verify the integrity and completeness of the performed backup.

    Args:
        backup_result: Result string from the backup task.

    Returns:
        str: Verification status.
    """
    logger = get_run_logger()
    logger.info("Verifying backup result: %s", backup_result)
    # Placeholder for verification logic
    return "verification_success"


@task
def backup_complete_task() -> None:
    """Final step indicating the backup pipeline has completed."""
    logger = get_run_logger()
    logger.info("Backup process completed")


@flow(name="database_backup_flow")
def backup_flow() -> None:
    """
    Orchestrates the backup pipeline:
    start → branch decision → appropriate backup → verification → completion.
    """
    start_backup_process()
    backup_type = date_check_task()

    # Conditional execution of the appropriate backup task
    if backup_type == "full":
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()

    # Verification runs after whichever backup task completed
    verify_future = verify_backup_task.submit(backup_future)

    # Ensure completion step runs after verification
    backup_complete_task.wait_for(verify_future)


if __name__ == "__main__":
    # Note: Deployment schedule (daily at midnight UTC) should be configured
    # via Prefect UI or deployment YAML; this script runs the flow locally.
    backup_flow()