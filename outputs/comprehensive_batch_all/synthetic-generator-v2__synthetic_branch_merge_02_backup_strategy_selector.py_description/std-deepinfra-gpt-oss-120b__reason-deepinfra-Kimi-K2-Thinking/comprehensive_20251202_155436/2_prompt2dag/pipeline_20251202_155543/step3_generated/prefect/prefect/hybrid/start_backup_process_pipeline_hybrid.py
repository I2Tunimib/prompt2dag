from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='start_backup_process', retries=2)
def start_backup_process():
    """Task: Start Backup Process"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='date_check_task', retries=2)
def date_check_task():
    """Task: Determine Backup Strategy"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='full_backup_task', retries=2)
def full_backup_task():
    """Task: Perform Full Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='incremental_backup_task', retries=2)
def incremental_backup_task():
    """Task: Perform Incremental Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='verify_backup_task', retries=2)
def verify_backup_task():
    """Task: Verify Backup Integrity"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='backup_complete', retries=2)
def backup_complete():
    """Task: Backup Workflow Completion"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="start_backup_process_pipeline", task_runner=SequentialTaskRunner())
def start_backup_process_pipeline():
    """Main pipeline flow implementing fanout-fanin pattern."""
    # Entry point
    start = start_backup_process()

    # Determine backup strategy
    date_check = date_check_task(wait_for=[start])

    # Fan‑out: run both backup types in parallel
    full = full_backup_task(wait_for=[date_check])
    incremental = incremental_backup_task(wait_for=[date_check])

    # Fan‑in: verify after both backups complete
    verify = verify_backup_task(wait_for=[full, incremental])

    # Completion step
    backup_complete(wait_for=[verify])


if __name__ == "__main__":
    start_backup_process_pipeline()