from dagster import op, job, in_process_executor, fs_io_manager, resource


@resource
def database_conn(_):
    """Placeholder resource for database connection."""
    return None


@op(
    name='wait_partition',
    description='Wait for Daily Partition',
)
def wait_partition(context):
    """Op: Wait for Daily Partition"""
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
    name='transform_orders',
    description='Transform Orders Data',
)
def transform_orders(context):
    """Op: Transform Orders Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_orders',
    description='Load Orders to Warehouse',
)
def load_orders(context):
    """Op: Load Orders to Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='wait_partition_pipeline',
    description='Database Partition Check ETL',
    executor_def=in_process_executor,
    resource_defs={
        'database_conn': database_conn,
        'io_manager': fs_io_manager,
    },
)
def wait_partition_pipeline():
    wait_partition() >> extract_incremental() >> transform_orders() >> load_orders()