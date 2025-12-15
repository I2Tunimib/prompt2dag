from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='wait_for_file',
    description='Wait for File',
)
def wait_for_file(context):
    """Op: Wait for File"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='validate_schema',
    description='Validate Schema',
)
def validate_schema(context):
    """Op: Validate Schema"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_db',
    description='Load to Database',
)
def load_db(context):
    """Op: Load to Database"""
    # Docker execution
    # Image: python:3.9
    pass

local_filesystem = ResourceDefinition.mock_resource()
postgresql_connection = ResourceDefinition.mock_resource()

@job(
    name="wait_for_file_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "local_filesystem": local_filesystem,
        "postgresql_connection": postgresql_connection
    }
)
def wait_for_file_pipeline():
    validate_schema_op = validate_schema(wait_for_file())
    load_db(validate_schema_op)