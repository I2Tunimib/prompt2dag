from dagster import job, op, in_process_executor, fs_io_manager, resource

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
    description='Load Database',
)
def load_db(context):
    """Op: Load Database"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def postgres_db():
    pass

@resource
def local_filesystem():
    pass

# Job Definition
@job(
    name="wait_for_file_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "postgres_db": postgres_db,
        "local_filesystem": local_filesystem
    },
    io_manager_def=fs_io_manager
)
def wait_for_file_pipeline():
    validate_schema_op = validate_schema(wait_for_file())
    load_db(validate_schema_op)