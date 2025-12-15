from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

# Task Definitions
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

# Job Definition
@job(
    name="wait_for_file_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "local_filesystem": ResourceDefinition.mock_resource(),
        "postgresql": ResourceDefinition.mock_resource(),
    },
)
def wait_for_file_pipeline():
    # Sequential pattern
    validate_schema_op = validate_schema.alias("validate_schema")
    load_db_op = load_db.alias("load_db")

    file_waited = wait_for_file()
    schema_validated = validate_schema_op(file_waited)
    load_db_op(schema_validated)