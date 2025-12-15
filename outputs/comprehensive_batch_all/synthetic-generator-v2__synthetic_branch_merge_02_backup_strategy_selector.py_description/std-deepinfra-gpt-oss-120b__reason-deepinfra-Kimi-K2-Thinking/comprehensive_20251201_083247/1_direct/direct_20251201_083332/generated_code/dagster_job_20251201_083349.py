from datetime import datetime, timezone

from dagster import (
    In,
    Nothing,
    Out,
    RetryPolicy,
    SkipReason,
    DefaultScheduleStatus,
    job,
    op,
    schedule,
)


@op
def start_backup_process(context):
    """Entry point for the backup pipeline."""
    context.log.info("Starting backup process.")


@op
def date_check_task(context) -> str:
    """
    Determines the day of the week for the scheduled execution.
    Returns the full name of the day (e.g., 'Monday', 'Saturday').
    """
    exec_time = getattr(context, "scheduled_execution_time", None)
    if exec_time is None:
        exec_time = datetime.now(timezone.utc)
    day_name = exec_time.strftime("%A")
    context.log.info(f"Execution day determined: {day_name}")
    return day_name


@op(
    out=Out(Nothing, is_required=False),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def full_backup_task(context, day_of_week: str):
    """
    Performs a full backup on Saturdays.
    Skips execution on any other day.
    """
    if day_of_week == "Saturday":
        context.log.info("Performing full database backup (Saturday).")
        # Placeholder for actual backup command, e.g., subprocess.run([...])
    else:
        raise SkipReason("Not Saturday; skipping full backup.")


@op(
    out=Out(Nothing, is_required=False),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def incremental_backup_task(context, day_of_week: str):
    """
    Performs an incremental backup on weekdays.
    Skips execution on Saturdays.
    """
    if day_of_week != "Saturday":
        context.log.info("Performing incremental database backup (weekday).")
        # Placeholder for actual backup command, e.g., subprocess.run([...])
    else:
        raise SkipReason("Saturday; skipping incremental backup.")


@op(
    ins={"full": In(Nothing, is_required=False), "inc": In(Nothing, is_required=False)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def verify_backup_task(context):
    """
    Verifies backup integrity after whichever backup path succeeded.
    Runs as long as at least one upstream backup succeeded.
    """
    context.log.info("Verifying backup integrity and completeness.")
    # Placeholder for verification logic, e.g., checksum validation


@op
def backup_complete(context):
    """Final step indicating the backup pipeline has finished."""
    context.log.info("Backup process completed successfully.")


@job
def backup_job():
    start = start_backup_process()
    day = date_check_task()
    full = full_backup_task(day)
    inc = incremental_backup_task(day)
    verify = verify_backup_task(full, inc)
    backup_complete(verify)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=backup_job,
    name="daily_backup_schedule",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"owner": "Backup team"},
)
def daily_backup_schedule(_context):
    """Schedule that triggers the backup_job daily without catchup."""
    return {}


if __name__ == "__main__":
    result = backup_job.execute_in_process()
    if result.success:
        print("Backup job executed successfully.")
    else:
        print("Backup job failed.")