from datetime import datetime, timedelta
import subprocess

from dagster import (
    Nothing,
    Out,
    In,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
    repository,
)


def _run_command(command: str) -> None:
    """Execute a shell command, raising an exception on failure."""
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    print(result.stdout)


@op(
    name="start_backup_process",
    out=Out(Nothing),
    description="Entry point of the backup pipeline.",
    tags={"owner": "Backup team"},
)
def start_backup_process() -> Nothing:
    # No operation needed; serves as a logical start.
    return None


@op(
    name="date_check_task",
    out=Out(dict),
    description="Determine backup strategy based on the current day of week.",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def date_check_task() -> dict:
    """Return a dict indicating whether a full backup should run.

    Returns:
        {"is_full_backup": True} on Saturday, otherwise {"is_full_backup": False}.
    """
    today = datetime.utcnow()
    is_full = today.weekday() == 5  # Monday=0, ..., Saturday=5
    print(f"Today is {today.strftime('%A')}; full backup: {is_full}")
    return {"is_full_backup": is_full}


@op(
    name="full_backup_task",
    ins={"strategy": In(dict)},
    out=Out(Nothing),
    description="Execute a full database backup (Saturday path).",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def full_backup_task(strategy: dict) -> Nothing:
    if not strategy.get("is_full_backup", False):
        print("Skipping full backup; not Saturday.")
        return None

    command = "echo Running full database backup..."
    print("Starting full backup.")
    _run_command(command)
    print("Full backup completed.")
    return None


@op(
    name="incremental_backup_task",
    ins={"strategy": In(dict)},
    out=Out(Nothing),
    description="Execute an incremental database backup (weekday path).",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def incremental_backup_task(strategy: dict) -> Nothing:
    if strategy.get("is_full_backup", False):
        print("Skipping incremental backup; today is Saturday.")
        return None

    command = "echo Running incremental database backup..."
    print("Starting incremental backup.")
    _run_command(command)
    print("Incremental backup completed.")
    return None


@op(
    name="verify_backup_task",
    ins={
        "full": In(Nothing, optional=True),
        "incremental": In(Nothing, optional=True),
    },
    out=Out(Nothing),
    description=(
        "Validate backup integrity. Runs when at least one backup path succeeded."
    ),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def verify_backup_task(full: Nothing = None, incremental: Nothing = None) -> Nothing:
    # In Dagster, this op will be scheduled after both backup ops.
    # The optional inputs allow execution even if one path was skipped.
    command = "echo Verifying backup integrity..."
    print("Starting backup verification.")
    _run_command(command)
    print("Backup verification completed.")
    return None


@op(
    name="backup_complete",
    out=Out(Nothing),
    description="Final step marking successful pipeline completion.",
    tags={"owner": "Backup team"},
)
def backup_complete() -> Nothing:
    print("Backup pipeline completed successfully.")
    return None


@job(
    description="Daily backup pipeline with conditional full or incremental backup.",
    tags={"owner": "Backup team"},
)
def backup_job():
    start = start_backup_process()
    strategy = date_check_task()
    full = full_backup_task(strategy)
    inc = incremental_backup_task(strategy)
    verify = verify_backup_task(full=full, incremental=inc)
    backup_complete(verify)


daily_backup_schedule = ScheduleDefinition(
    job=backup_job,
    cron_schedule="@daily",
    execution_timezone="UTC",
    description="Runs the backup_job once per day.",
    tags={"owner": "Backup team"},
    # No catchup behavior in Dagster; schedules run from the next tick forward.
)


@repository
def backup_repository():
    return [backup_job, daily_backup_schedule]


if __name__ == "__main__":
    result = backup_job.execute_in_process()
    assert result.success