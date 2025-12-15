from dagster import op, job, schedule, fs_io_manager

@op(
    name='start_backup_process',
    description='Start Backup Process',
)
def start_backup_process(context):
    """Op: Start Backup Process"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='date_check_task',
    description='Determine Backup Strategy',
)
def date_check_task(context):
    """Op: Determine Backup Strategy"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='full_backup_task',
    description='Perform Full Backup',
)
def full_backup_task(context):
    """Op: Perform Full Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='incremental_backup_task',
    description='Perform Incremental Backup',
)
def incremental_backup_task(context):
    """Op: Perform Incremental Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='verify_backup_task',
    description='Verify Backup Integrity',
)
def verify_backup_task(context):
    """Op: Verify Backup Integrity"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='backup_complete',
    description='Backup Workflow Completion',
)
def backup_complete(context):
    """Op: Backup Workflow Completion"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='start_backup_process_pipeline',
    description='No description provided.',
    resource_defs={'io_manager': fs_io_manager},
)
def start_backup_process_pipeline():
    start = start_backup_process()
    date = date_check_task()
    full = full_backup_task()
    incremental = incremental_backup_task()
    verify = verify_backup_task()
    complete = backup_complete()

    # Define fan‑out / fan‑in dependencies
    start >> date
    date >> [full, incremental]
    [full, incremental] >> verify
    verify >> complete


@schedule(
    cron_schedule='@daily',
    job=start_backup_process_pipeline,
    execution_timezone='UTC',
)
def daily_backup_schedule(_context):
    return {}