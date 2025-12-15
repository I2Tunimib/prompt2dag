from dagster import op, job, in_process_executor, fs_io_manager

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
    name='determine_backup_strategy',
    description='Determine Backup Strategy',
)
def determine_backup_strategy(context):
    """Op: Determine Backup Strategy"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='full_backup',
    description='Full Backup',
)
def full_backup(context):
    """Op: Full Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='incremental_backup',
    description='Incremental Backup',
)
def incremental_backup(context):
    """Op: Incremental Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='verify_backup',
    description='Verify Backup',
)
def verify_backup(context):
    """Op: Verify Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='backup_complete',
    description='Backup Complete',
)
def backup_complete(context):
    """Op: Backup Complete"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="start_backup_process_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def start_backup_process_pipeline():
    start = start_backup_process()
    strategy = determine_backup_strategy(start)
    full = full_backup(strategy)
    incr = incremental_backup(strategy)
    verify = verify_backup(full, incr)
    backup_complete(verify)