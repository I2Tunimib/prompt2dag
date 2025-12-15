from prefect import flow, task

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

@task(name='validate_backup_integrity', retries=2)
def validate_backup_integrity():
    """Task: Validate Backup Integrity"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='finalize_backup_workflow', retries=2)
def finalize_backup_workflow():
    """Task: Finalize Backup Workflow"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="start_backup_process_pipeline")
def start_backup_process_pipeline():
    # Entry point
    start = start_backup_process()
    
    # Determine strategy after start
    strategy = determine_backup_strategy(wait_for=[start])
    
    # Fan‑out: run both backup types in parallel
    full_backup = perform_full_backup(wait_for=[strategy])
    incremental_backup = perform_incremental_backup(wait_for=[strategy])
    
    # Fan‑in: validate after both backups complete
    validation = validate_backup_integrity(wait_for=[full_backup, incremental_backup])
    
    # Final step
    finalize = finalize_backup_workflow(wait_for=[validation])
    
    return finalize

if __name__ == "__main__":
    start_backup_process_pipeline()