from datetime import datetime, timezone

from prefect import flow, task, get_run_logger


@task(retries=2, retry_delay_seconds=300)
def start_backup_process() -> None:
    """Entry point for the backup pipeline."""
    logger = get_run_logger()
    logger.info("Starting backup process.")


@task(retries=2, retry_delay_seconds=300)
def date_check_task() -> str:
    """
    Determine which backup strategy to use based on the current UTC day of week.

    Returns:
        str: ``"full"`` for Saturday, otherwise ``"incremental"``.
    """
    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    day_of_week = now.weekday()  # Monday=0, Sunday=6
    logger.info(f"Current UTC datetime: {now.isoformat()}, day of week: {day_of_week}")

    if day_of_week == 5:  # Saturday
        logger.info("Selected full backup strategy.")
        return "full"
    logger.info("Selected incremental backup strategy.")
    return "incremental"


@task(retries=2, retry_delay_seconds=300)
def full_backup_task() -> str:
    """Perform a full database backup."""
    logger = get_run_logger()
    logger.info("Executing full database backup...")
    # Placeholder for actual backup command, e.g., subprocess.run([...], check=True)
    logger.info("Full backup completed successfully.")
    return "full"


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task() -> str:
    """Perform an incremental database backup."""
    logger = get_run_logger()
    logger.info("Executing incremental database backup...")
    # Placeholder for actual backup command
    logger.info("Incremental backup completed successfully.")
    return "incremental"


@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_type: str) -> bool:
    """
    Verify the integrity of the completed backup.

    Args:
        backup_type: The type of backup that was performed ("full" or "incremental").

    Returns:
        bool: True if verification succeeds.
    """
    logger = get_run_logger()
    logger.info(f"Verifying {backup_type} backup integrity...")
    # Placeholder for verification logic
    logger.info(f"{backup_type.capitalize()} backup verification succeeded.")
    return True


@task(retries=2, retry_delay_seconds=300)
def backup_complete() -> None:
    """Final step indicating the backup pipeline has finished."""
    logger = get_run_logger()
    logger.info("Backup process completed successfully.")


@flow
def backup_flow() -> None:
    """
    Orchestrates the backup pipeline.

    Schedule (to be configured in deployment):
        - Daily execution starting 2024-01-01
        - No catchup, owner: Backup team
    """
    start_backup_process()
    backup_type = date_check_task()

    # Run the appropriate backup task concurrently (only one will be submitted)
    if backup_type == "full":
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()

    # Verify after the backup completes
    verify_future = verify_backup_task.submit(backup_type, wait_for=[backup_future])

    # Mark completion after verification
    backup_complete(wait_for=[verify_future])


if __name__ == "__main__":
    backup_flow()