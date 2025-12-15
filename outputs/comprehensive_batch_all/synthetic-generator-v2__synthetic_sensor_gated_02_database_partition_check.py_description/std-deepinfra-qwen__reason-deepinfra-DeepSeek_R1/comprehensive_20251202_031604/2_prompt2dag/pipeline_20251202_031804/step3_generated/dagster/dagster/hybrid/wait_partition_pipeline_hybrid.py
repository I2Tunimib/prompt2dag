from dagster import job, op, in_process_executor, fs_io_manager, resource

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

@job(
    name="wait_partition_pipeline",
    description="This is a sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.",
    executor_def=in_process_executor,
    resource_defs={
        "database_conn": resource(config_schema={"conn_str": str})(lambda init_context: init_context.resource_config["conn_str"]),
        "data_warehouse_conn": resource(config_schema={"conn_str": str})(lambda init_context: init_context.resource_config["conn_str"]),
    },
    io_manager_def=fs_io_manager,
)
def wait_partition_pipeline():
    load(transform(extract_incremental(wait_partition())))