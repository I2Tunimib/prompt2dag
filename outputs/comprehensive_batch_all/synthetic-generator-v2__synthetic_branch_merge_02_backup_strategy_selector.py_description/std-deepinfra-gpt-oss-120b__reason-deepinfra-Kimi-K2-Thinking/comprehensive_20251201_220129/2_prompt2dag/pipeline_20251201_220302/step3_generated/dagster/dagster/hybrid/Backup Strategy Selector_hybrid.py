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
    name='perform_full_backup',
    description='Perform Full Backup',
)
def perform_full_backup(context):
    """Op: Perform Full Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='perform_incremental_backup',
    description='Perform Incremental Backup',
)
def perform_incremental_backup(context):
    """Op: Perform Incremental Backup"""
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
    name='finalize_backup',
    description='Finalize Backup',
)
def finalize_backup(context):
    """Op: Finalize Backup"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='backup_strategy_selector',
    description='Comprehensive pipeline that selects a backup strategy (full or incremental) based on the day of week, using a branch‑merge pattern.',
    executor_def=in_process_executor,
    resource_defs={'io_manager': fs_io_manager},
)
def backup_strategy_selector():
    # Entry point
    start = start_backup_process()
    # Determine strategy after start
    strategy = determine_backup_strategy(start)
    # Fan‑out to both backup types
    full = perform_full_backup(strategy)
    incremental = perform_incremental_backup(strategy)
    # Fan‑in verification
    verification = verify_backup(full, incremental)
    # Finalize
    finalize_backup(verification)