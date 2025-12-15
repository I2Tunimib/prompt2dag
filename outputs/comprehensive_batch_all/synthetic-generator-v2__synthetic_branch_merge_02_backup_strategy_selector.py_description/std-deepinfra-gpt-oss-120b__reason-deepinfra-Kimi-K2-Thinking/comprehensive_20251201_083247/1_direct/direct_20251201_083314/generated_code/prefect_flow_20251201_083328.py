from datetime import datetime

import subprocess

from prefect import flow, task, get_run_logger
from prefect.states import State


@task
def start_backup_process():
    """Entry point for the backup workflow."""
    logger = get_run_logger()
    logger.info("Starting backup process.")


@task
def date_check_task() -> str:
    """
    Determine which backup strategy to use based on the current UTC day.

    Returns:
        str: ``'full'`` for Saturday, otherwise ``'incremental'``.
    """
    now = datetime.utcnow()
    # Monday == 0, Saturday == 5, Sunday == 6
    if now.weekday() == 5:
        return "full"
    return "incremental"


@task(retries=2, retry_delay_seconds=300)
def full_backup_task() -> None:
    """Execute a full database backup."""
    logger = get_run_logger()
    cmd = "echo 'Running full backup...'"
    logger.info(f"Executing command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(result.stdout.strip())
    if result.returncode != 0:
        raise RuntimeError(f"Full backup failed: {result.stderr.strip()}")


@task(retries=2, retry_delay_seconds=300)
def incremental_backup_task() -> None:
    """Execute an incremental database backup."""
    logger = get_run_logger()
    cmd = "echo 'Running incremental backup...'"
    logger.info(f"Executing command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(result.stdout.strip())
    if result.returncode != 0:
        raise RuntimeError(f"Incremental backup failed: {result.stderr.strip()}")


@task
def verify_backup_task(backup_state: State) -> None:
    """
    Verify backup integrity after a successful backup task.

    Mimics Airflow's ``none_failed_min_one_success`` trigger rule:
    verification runs if the upstream backup succeeded.
    """
    logger = get_run_logger()
    if backup_state.is_failed():
        raise RuntimeError("Backup task failed; verification aborted.")
    logger.info("Backup succeeded – starting verification.")
    # Placeholder for real verification logic
    logger.info("Backup verification completed successfully.")


@task
def backup_complete():
    """Final step indicating the workflow has finished."""
    logger = get_run_logger()
    logger.info("Backup process completed.")


@flow
def backup_flow():
    """
    Orchestrates the backup workflow:
    start → decide strategy → run appropriate backup → verify → finish.
    """
    start_backup_process()
    strategy = date_check_task()

    if strategy == "full":
        backup_future = full_backup_task.submit()
    else:
        backup_future = incremental_backup_task.submit()

    # Wait for the backup task to finish and capture its state
    backup_state = backup_future.wait()

    verify_backup_task(backup_state)
    backup_complete()


if __name__ == "__main__":
    # For local execution; in production, configure a daily deployment.
    backup_flow()