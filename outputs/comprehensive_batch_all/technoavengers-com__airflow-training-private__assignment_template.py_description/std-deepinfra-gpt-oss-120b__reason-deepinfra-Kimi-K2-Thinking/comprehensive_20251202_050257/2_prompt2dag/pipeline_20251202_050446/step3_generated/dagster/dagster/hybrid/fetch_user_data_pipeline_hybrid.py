from dagster import (
    op,
    job,
    schedule,
    fs_io_manager,
    ResourceDefinition,
    in_process_executor,
)


@op(
    name="fetch_user_data",
    description="Fetch User Data from API",
)
def fetch_user_data(context):
    """Op: Fetch User Data from API"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="transform_user_data",
    description="Transform User Data",
)
def transform_user_data(context, fetch_user_data):
    """Op: Transform User Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="create_user_table",
    description="Create Users Table in PostgreSQL",
)
def create_user_table(context, transform_user_data):
    """Op: Create Users Table in PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="insert_user_data",
    description="Insert User Data into PostgreSQL",
)
def insert_user_data(context, create_user_table):
    """Op: Insert User Data into PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="fetch_user_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "reqres": ResourceDefinition.hardcoded_resource({}),
        "postgres": ResourceDefinition.hardcoded_resource({}),
    },
)
def fetch_user_data_pipeline():
    fetched = fetch_user_data()
    transformed = transform_user_data(fetched)
    table_created = create_user_table(transformed)
    insert_user_data(table_created)


@schedule(
    cron_schedule="@daily",
    job=fetch_user_data_pipeline,
    execution_timezone="UTC",
)
def fetch_user_data_schedule():
    return {}