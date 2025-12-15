from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor, daily_schedule

# Task Definitions
@op(
    name='wait_partition',
    description='Wait for Partition',
)
def wait_partition(context):
    """Op: Wait for Partition"""
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
    description='Load Orders Data',
)
def load(context):
    """Op: Load Orders Data"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="wait_partition_pipeline",
    description="Comprehensive Pipeline Description for Database Partition Check ETL",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "database_conn": ResourceDefinition.hardcoded_resource(None, "database_conn"),
        "data_warehouse_conn": ResourceDefinition.hardcoded_resource(None, "data_warehouse_conn")
    }
)
def wait_partition_pipeline():
    load(transform(extract_incremental(wait_partition())))

# Schedule Definition
@daily_schedule(
    pipeline_name="wait_partition_pipeline",
    start_date="2023-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
)
def wait_partition_pipeline_schedule(context):
    return {}

# Ensure the schedule is enabled
wait_partition_pipeline_schedule.is_running = True