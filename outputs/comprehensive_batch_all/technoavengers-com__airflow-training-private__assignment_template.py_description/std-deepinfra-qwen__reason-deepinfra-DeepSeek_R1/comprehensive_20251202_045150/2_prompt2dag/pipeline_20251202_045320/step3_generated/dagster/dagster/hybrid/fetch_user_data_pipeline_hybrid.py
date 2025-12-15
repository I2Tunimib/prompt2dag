from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

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
    name='process_user_data',
    description='Process User Data',
)
def process_user_data(context):
    """Op: Process User Data"""
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

@job(
    name='fetch_user_data_pipeline',
    description='No description provided.',
    executor_def=in_process_executor,
    resource_defs={
        'io_manager': fs_io_manager,
        'postgres': ResourceDefinition.hardcoded_resource(None, 'postgres'),
        'reqres': ResourceDefinition.hardcoded_resource(None, 'reqres')
    }
)
def fetch_user_data_pipeline():
    fetch_user_data_output = fetch_user_data()
    process_user_data_output = process_user_data(fetch_user_data_output)
    create_user_table_output = create_user_table(process_user_data_output)
    insert_user_data(create_user_table_output)