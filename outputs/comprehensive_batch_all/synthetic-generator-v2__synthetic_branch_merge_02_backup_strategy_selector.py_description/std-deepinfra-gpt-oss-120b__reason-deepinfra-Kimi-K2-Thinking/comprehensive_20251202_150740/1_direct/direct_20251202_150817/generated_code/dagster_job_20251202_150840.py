from datetime import datetime

from dagster import (
    DefaultScheduleStatus,
    RetryPolicy,
    JobDefinition,
    Job,
    op,
    job,
    schedule,
)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Entry point for the backup pipeline.",
)
def start_backup_process() -> None:
    """Placeholder op to mark the start of the pipeline."""
    return None


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Determine whether to run a full or incremental backup based on the execution date.",
)
def date_check(context) -> bool:
    """
    Returns True for a full backup (Saturday) and False for an incremental backup (weekday).
    """
    exec_date: datetime = (
        context.scheduled_execution_time if context.scheduled_execution_time else datetime.utcnow()
    )
    # In Python, Monday is 0 and Sunday is 6; Saturday is 5.
    return exec_date.weekday() == 5


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Execute the appropriate backup command based on the strategy.",
)
def backup(is_full: bool) -> str:
    """
    Simulates execution of a backup command.
    Returns a string identifier for downstream verification.
    """
    if is_full:
        # Placeholder for a full backup command, e.g., a Bash script.
        return "full_backup_done"
    else:
        # Placeholder for an incremental backup command.
        return "incremental_backup_done"


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Validate the integrity and completeness of the performed backup.",
)
def verify_backup(backup_result: str) -> str:
    """
    Simulates backup verification.
    """
    # In a real implementation, this would run verification logic.
    return f"verified_{backup_result}"


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Marks the successful completion of the backup pipeline.",
)
def backup_complete(_: str) -> None:
    """Final placeholder op."""
    return None


@job(
    tags={"owner": "Backup team"},
    description="Daily backup job with conditional full/incremental strategy.",
)
def backup_job() -> None:
    """
    Orchestrates the backup pipeline:
    start → date_check → backup → verify_backup → backup_complete
    """
    start = start_backup_process()
    is_full = date_check(start)
    backup_result = backup(is_full)
    verification = verify_backup(backup_result)
    backup_complete(verification)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=backup_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"owner": "Backup team"},
    description="Schedule to run the backup_job daily.",
)
def daily_backup_schedule():
    """Schedule definition for the daily backup job."""
    return {}


if __name__ == "__main__":
    result = backup_job.execute_in_process()
    print(f"Job succeeded: {result.success}")