from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

# Task Definitions
@op(
    name='get_airvisual_data_hourly',
    description='Fetch AirVisual Data',
)
def get_airvisual_data_hourly(context):
    """Op: Fetch AirVisual Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='read_data_airvisual',
    description='Read and Validate AirVisual Data',
)
def read_data_airvisual(context):
    """Op: Read and Validate AirVisual Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_data_airvisual_to_postgresql',
    description='Load AirVisual Data to PostgreSQL',
)
def load_data_airvisual_to_postgresql(context):
    """Op: Load AirVisual Data to PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="airvisual_pipeline_lat_long_v1",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "airvisual_api": ResourceDefinition.hardcoded_resource(None, "airvisual_api"),
        "postgres_conn": ResourceDefinition.hardcoded_resource(None, "postgres_conn"),
        "local_filesystem": fs_io_manager,
    },
)
def airvisual_pipeline_lat_long_v1():
    # Sequential pattern
    read_data_airvisual_op = read_data_airvisual(get_airvisual_data_hourly())
    load_data_airvisual_to_postgresql(read_data_airvisual_op)