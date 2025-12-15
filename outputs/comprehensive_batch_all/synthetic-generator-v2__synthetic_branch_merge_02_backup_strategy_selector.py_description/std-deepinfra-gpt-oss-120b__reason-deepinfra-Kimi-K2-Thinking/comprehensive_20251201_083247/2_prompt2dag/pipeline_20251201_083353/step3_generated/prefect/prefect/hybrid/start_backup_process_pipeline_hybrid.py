from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='start_backup_process', retries=2)
def start_backup_process():
    """Task: Start Backup Process"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='determine_backup_strategy', retries=2)
def determine_backup_strategy():
    """Task: Determine Backup Strategy"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='full_backup', retries=2)
def full_backup():
    """Task: Full Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='incremental_backup', retries=2)
def incremental_backup():
    """Task: Incremental Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='verify_backup', retries=2)
def verify_backup():
    """Task: Verify Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='backup_complete', retries=2)
def backup_complete():
    """Task: Backup Complete"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="start_backup_process_pipeline", task_runner=SequentialTaskRunner())
def start_backup_process_pipeline():
    # Entry point
    start = start_backup_process()

    # Determine strategy after start
    strategy = determine_backup_strategy(wait_for=[start])

    # Fan‑out: run both backup types after strategy is known
    full = full_backup(wait_for=[strategy])
    incremental = incremental_backup(wait_for=[strategy])

    # Fan‑in: verify after both backups complete
    verification = verify_backup(wait_for=[full, incremental])

    # Final step
    backup_complete(wait_for=[verification])


if __name__ == "__main__":
    start_backup_process_pipeline()