from dagster import op, job, InProcessExecutor, fs_io_manager, ResourceDefinition

# -------------------------------------------------
# Task Definitions (use exactly as provided)
# -------------------------------------------------

@op(
    name='wait_partition',
    description='Wait for Daily Orders Partition',
)
def wait_partition(context):
    """Op: Wait for Daily Orders Partition"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='extract_incremental',
    description='Extract Incremental Orders',
)
def extract_incremental(context):
    """Op: Extract Incremental Orders"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform',
    description='Transform Orders Data',
)
def transform(context):
    """Op: Transform Orders Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load',
    description='Load Orders to Data Warehouse',
)
def load(context):
    """Op: Load Orders to Data Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass


# -------------------------------------------------
# Job Definition
# -------------------------------------------------

@job(
    name="wait_partition_pipeline",
    description="No description provided.",
    executor_def=InProcessExecutor(),
    resource_defs={"database_conn": ResourceDefinition.hardcoded_resource(None)},
    io_manager_defs={"fs_io_manager": fs_io_manager},
    default_io_manager_key="fs_io_manager",
)
def wait_partition_pipeline():
    """Sequential pipeline: wait_partition -> extract_incremental -> transform -> load"""
    wait = wait_partition()
    extract = extract_incremental()
    transformed = transform()
    loaded = load()

    # Define sequential dependencies without passing data
    wait >> extract >> transformed >> loaded