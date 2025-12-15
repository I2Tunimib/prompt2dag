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
    name='validate_backup_integrity',
    description='Validate Backup Integrity',
)
def validate_backup_integrity(context):
    """Op: Validate Backup Integrity"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='finalize_backup_workflow',
    description='Finalize Backup Workflow',
)
def finalize_backup_workflow(context):
    """Op: Finalize Backup Workflow"""
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
    strategy = determine_backup_strategy()
    full = perform_full_backup()
    incremental = perform_incremental_backup()
    validate = validate_backup_integrity()
    finalize = finalize_backup_workflow()

    # Define dependencies (fanout/fanin pattern)
    start >> strategy
    strategy >> full
    strategy >> incremental
    full >> validate
    incremental >> validate
    validate >> finalize