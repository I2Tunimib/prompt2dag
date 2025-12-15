from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition, schedule

# Pre-Generated Task Definitions (use exactly as provided)

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


# Resource definition placeholder for database connection
database_conn = ResourceDefinition.hardcoded_resource(None)


@job(
    name="database_partition_check_etl",
    description="Sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.",
    executor_def=in_process_executor,
    resource_defs={"database_conn": database_conn, "io_manager": fs_io_manager},
)
def database_partition_check_etl():
    # Sequential wiring of ops
    load_orders(
        transform_orders(
            extract_incremental(
                wait_partition()
            )
        )
    )


# Daily schedule (enabled)
@schedule(cron_schedule="0 0 * * *", job=database_partition_check_etl, description="Daily execution of the Database Partition Check ETL")
def daily_database_partition_check_etl_schedule():
    return {}