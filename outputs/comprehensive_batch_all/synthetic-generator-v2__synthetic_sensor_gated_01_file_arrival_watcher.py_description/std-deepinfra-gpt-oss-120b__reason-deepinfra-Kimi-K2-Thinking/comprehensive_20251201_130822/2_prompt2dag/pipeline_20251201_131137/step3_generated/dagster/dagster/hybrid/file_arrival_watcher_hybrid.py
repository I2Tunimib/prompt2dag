from dagster import (
    op,
    job,
    in_process_executor,
    fs_io_manager,
    ResourceDefinition,
    schedule,
    DefaultScheduleStatus,
)

# Placeholder resources (replace with real implementations as needed)
postgres_local = ResourceDefinition.hardcoded_resource(None)
fs_incoming = ResourceDefinition.hardcoded_resource(None)


@op(
    name="wait_for_file",
    description="Wait for Transaction File",
)
def wait_for_file(context):
    """Op: Wait for Transaction File"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="validate_schema",
    description="Validate Transaction File Schema",
)
def validate_schema(context):
    """Op: Validate Transaction File Schema"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="load_db",
    description="Load Validated Transactions to PostgreSQL",
)
def load_db(context):
    """Op: Load Validated Transactions to PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL.",
    executor_def=in_process_executor,
    resource_defs={
        "postgres_local": postgres_local,
        "fs_incoming": fs_incoming,
        "io_manager": fs_io_manager,
    },
)
def file_arrival_watcher():
    # Sequential execution: wait_for_file -> validate_schema -> load_db
    wait_for_file().then(validate_schema).then(load_db)


@schedule(
    cron_schedule="@daily",
    job=file_arrival_watcher,
    description="Daily execution of the file_arrival_watcher job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def file_arrival_watcher_schedule(_context):
    return {}