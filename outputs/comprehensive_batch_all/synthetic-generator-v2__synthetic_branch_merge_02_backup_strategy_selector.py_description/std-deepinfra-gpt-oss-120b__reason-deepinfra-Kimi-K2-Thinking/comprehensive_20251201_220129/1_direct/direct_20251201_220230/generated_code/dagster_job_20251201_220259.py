from datetime import datetime

from dagster import (
    Nothing,
    In,
    Out,
    RetryPolicy,
    DefaultScheduleStatus,
    job,
    op,
    schedule,
)


@op(out=Out(Nothing), retry_policy=RetryPolicy(max_retries=2, delay=300))
def start_backup_process():
    """Entry point of the backup pipeline."""
    pass


@op(out=Out(bool), retry_policy=RetryPolicy(max_retries=2, delay=300))
def date_check_task() -> bool:
    """
    Determines whether today is Saturday.

    Returns:
        bool: True if today is Saturday (full backup), False otherwise (incremental backup).
    """
    today = datetime.utcnow().date()
    # Monday is 0, Saturday is 5
    is_full = today.weekday() == 5
    return is_full


@op(
    ins={"is_full": In(bool)},
    out=Out(bool),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def full_backup_task(is_full: bool) -> bool:
    """
    Executes a full backup when the day is Saturday.

    Args:
        is_full: Indicator from ``date_check_task``.

    Returns:
        bool: True if the backup was performed, False otherwise.
    """
    if not is_full:
        return False
    # Placeholder for actual backup command, e.g., subprocess.run([...])
    return True


@op(
    ins={"is_full": In(bool)},
    out=Out(bool),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def incremental_backup_task(is_full: bool) -> bool:
    """
    Executes an incremental backup on weekdays.

    Args:
        is_full: Indicator from ``date_check_task``.

    Returns:
        bool: True if the backup was performed, False otherwise.
    """
    if is_full:
        return False
    # Placeholder for actual backup command, e.g., subprocess.run([...])
    return True


@op(
    ins={"full_success": In(bool), "inc_success": In(bool)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def verify_backup_task(full_success: bool, inc_success: bool):
    """
    Verifies backup integrity after at least one backup path succeeds.

    Raises:
        Exception: If neither backup succeeded.
    """
    if not (full_success or inc_success):
        raise Exception("No backup succeeded; verification cannot proceed.")
    # Placeholder for verification logic.
    pass


@op(out=Out(Nothing), retry_policy=RetryPolicy(max_retries=2, delay=300))
def backup_complete():
    """Exit point of the backup pipeline."""
    pass


@job
def backup_job():
    """Orchestrates the backup workflow with branching and merging."""
    start_backup_process()
    is_full = date_check_task()
    full_success = full_backup_task(is_full)
    inc_success = incremental_backup_task(is_full)
    verify_backup_task(full_success, inc_success)
    backup_complete()


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=backup_job,
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="UTC",
)
def daily_backup_schedule():
    """Daily schedule for the backup job."""
    return {}


if __name__ == "__main__":
    result = backup_job.execute_in_process()
    if result.success:
        print("Backup job completed successfully.")
    else:
        print("Backup job failed.")