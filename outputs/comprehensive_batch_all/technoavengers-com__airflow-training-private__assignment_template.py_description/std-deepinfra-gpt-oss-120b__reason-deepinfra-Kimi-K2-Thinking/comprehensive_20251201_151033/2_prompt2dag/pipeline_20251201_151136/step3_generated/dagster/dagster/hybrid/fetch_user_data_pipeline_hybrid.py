from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition

# Pre-Generated Task Definitions (use exactly as provided)

@op(
    name='fetch_user_data',
    description='Fetch User Data',
)
def fetch_user_data(context):
    """Op: Fetch User Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform_user_data',
    description='Transform User Data',
)
def transform_user_data(context):
    """Op: Transform User Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='create_user_table',
    description='Create User Table',
)
def create_user_table(context):
    """Op: Create User Table"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='insert_user_data',
    description='Insert User Data',
)
def insert_user_data(context):
    """Op: Insert User Data"""
    # Docker execution
    # Image: python:3.9
    pass


# Job definition with sequential pattern and required resources

@job(
    name="fetch_user_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "reqres": ResourceDefinition.hardcoded_resource(None),
        "postgres": ResourceDefinition.hardcoded_resource(None),
    },
)
def fetch_user_data_pipeline():
    (
        fetch_user_data()
        .then(transform_user_data())
        .then(create_user_table())
        .then(insert_user_data())
    )