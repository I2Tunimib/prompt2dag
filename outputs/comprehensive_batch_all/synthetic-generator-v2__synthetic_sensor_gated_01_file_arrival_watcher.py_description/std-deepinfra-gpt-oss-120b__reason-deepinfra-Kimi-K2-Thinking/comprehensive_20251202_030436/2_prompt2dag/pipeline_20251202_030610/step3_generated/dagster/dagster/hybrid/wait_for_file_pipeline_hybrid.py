from dagster import op, job, ResourceDefinition, fs_io_manager, in_process_executor, schedule

# ----------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# ----------------------------------------------------------------------
@op(
    name='wait_for_file',
    description='Wait for Transaction File',
)
def wait_for_file(context):
    """Op: Wait for Transaction File"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='validate_schema',
    description='Validate Transaction File Schema',
)
def validate_schema(context):
    """Op: Validate Transaction File Schema"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_db',
    description='Load Transactions into PostgreSQL',
)
def load_db(context):
    """Op: Load Transactions into PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Resource Stubs (placeholders for required resources)
# ----------------------------------------------------------------------
postgres_local = ResourceDefinition.hardcoded_resource(None)
fs_incoming = ResourceDefinition.hardcoded_resource(None)


# ----------------------------------------------------------------------
# Job Definition
# ----------------------------------------------------------------------
@job(
    name="wait_for_file_pipeline",
    description=(
        "Sensor‑gated pipeline that monitors daily transaction file arrivals, "
        "validates the file schema, and loads the data into a PostgreSQL table. "
        "The workflow is linear: a FileSensor gates the process, followed by a "
        "Python‑based schema validation and a Python‑based load to PostgreSQL."
    ),
    executor_def=in_process_executor,
    resource_defs={
        "postgres_local": postgres_local,
        "fs_incoming": fs_incoming,
        "io_manager": fs_io_manager,
    },
)
def wait_for_file_pipeline():
    # Sequential execution: wait_for_file → validate_schema → load_db
    wait_for_file()
    validate_schema()
    load_db()


# ----------------------------------------------------------------------
# Schedule (daily)
# ----------------------------------------------------------------------
@schedule(
    cron_schedule="@daily",
    job=wait_for_file_pipeline,
    execution_timezone="UTC",
)
def wait_for_file_pipeline_schedule(_context):
    return {}