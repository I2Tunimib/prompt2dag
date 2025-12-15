from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

@task(name='start_backup_process', retries=0)
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

@task(name='perform_full_backup', retries=2)
def perform_full_backup():
    """Task: Perform Full Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='perform_incremental_backup', retries=2)
def perform_incremental_backup():
    """Task: Perform Incremental Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='verify_backup', retries=2)
def verify_backup():
    """Task: Verify Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='finalize_backup', retries=2)
def finalize_backup():
    """Task: Finalize Backup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="backup_strategy_selector", task_runner=SequentialTaskRunner())
def backup_strategy_selector():
    # Entry point
    start = start_backup_process.submit()
    
    # Determine which backup strategy to use
    strategy = determine_backup_strategy.submit(wait_for=[start])
    
    # Fan‑out: run both backup types in parallel (or sequentially under SequentialTaskRunner)
    full_backup = perform_full_backup.submit(wait_for=[strategy])
    incremental_backup = perform_incremental_backup.submit(wait_for=[strategy])
    
    # Fan‑in: verify after both backup tasks complete
    verification = verify_backup.submit(wait_for=[full_backup, incremental_backup])
    
    # Finalize the backup process
    finalization = finalize_backup.submit(wait_for=[verification])
    
    return finalization

if __name__ == "__main__":
    backup_strategy_selector()