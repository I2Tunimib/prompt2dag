from dagster import (
    op,
    job,
    in_process_executor,
    fs_io_manager,
    ResourceDefinition,
    schedule,
    RunRequest,
)

# ----------------------------------------------------------------------
# Resource definitions (placeholders)
# ----------------------------------------------------------------------
fs_local = ResourceDefinition.hardcoded_resource({})
postgres_local = ResourceDefinition.hardcoded_resource({})

# ----------------------------------------------------------------------
# Task definitions (use exactly as provided)
# ----------------------------------------------------------------------
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
    description="Load Transactions into PostgreSQL",
)
def load_db(context):
    """Op: Load Transactions into PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Job definition with sequential wiring
# ----------------------------------------------------------------------
@job(
    name="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data to PostgreSQL.",
    executor_def=in_process_executor,
    resource_defs={
        "fs_local": fs_local,
        "postgres_local": postgres_local,
        "io_manager": fs_io_manager,
    },
)
def file_arrival_watcher():
    # Sequential dependencies: wait_for_file -> validate_schema -> load_db
    wait_for_file() >> validate_schema() >> load_db()


# ----------------------------------------------------------------------
# Schedule (daily)
# ----------------------------------------------------------------------
@schedule(
    cron_schedule="@daily",
    job=file_arrival_watcher,
    description="Daily schedule for file_arrival_watcher",
)
def file_arrival_watcher_schedule(_context):
    return RunRequest(run_key=None, run_config={})