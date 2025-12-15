import datetime
import subprocess

from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def start_backup_process() -> str:
    """Entry point for the backup pipeline."""
    return "backup process started"


@task
def date_check_task(execution_date: datetime.datetime) -> bool:
    """
    Determine if the given execution date falls on a Saturday.

    Returns:
        True if Saturday, otherwise False.
    """
    # Monday is 0, Saturday is 5
    return execution_date.weekday() == 5


@task(retries=2, retry_delay_seconds=300)
def full_backup_task() -> str:
    """Execute a full database backup."""
    subprocess.run(["echo", "Running full database backup"], check=True)
    return "full backup completed"


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task() -> str:
    """Execute an incremental database backup."""
    subprocess.run(
        ["echo", "Running incremental database backup"], check=True
    )
    return "incremental backup completed"


@task(retries=2, retry_delay_seconds=300)
def verify_backup_task(backup_result: str) -> str:
    """Validate backup integrity and completeness."""
    subprocess.run(
        ["echo", f"Verifying backup result: {backup_result}"], check=True
    )
    return "backup verification completed"


@task
def backup_complete() -> str:
    """Final step indicating the backup pipeline has finished."""
    return "backup pipeline completed"


@flow
def backup_flow() -> str:
    """
    Orchestrates the backup pipeline.

    Schedule (to be configured in deployment):
        - Daily execution starting 2024-01-01
        - No catchup
        - Owner: Backup team
    """
    start_backup_process()

    # Use the current datetime as the execution date
    exec_date = datetime.datetime.now()
    is_saturday = date_check_task(exec_date)

    # Branch to the appropriate backup task
    if is_saturday:
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()

    # Verify the backup after the selected task completes
    verification_future = verify_backup_task.submit(backup_future.result())

    # End of pipeline
    backup_complete()

    return verification_future.result()


if __name__ == "__main__":
    backup_flow()