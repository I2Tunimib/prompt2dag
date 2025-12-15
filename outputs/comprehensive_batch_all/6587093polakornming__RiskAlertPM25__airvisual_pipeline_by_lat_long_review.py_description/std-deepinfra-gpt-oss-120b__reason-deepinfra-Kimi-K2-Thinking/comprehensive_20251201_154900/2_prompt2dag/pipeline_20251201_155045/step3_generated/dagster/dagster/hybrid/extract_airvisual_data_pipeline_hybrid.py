from dagster import op, job, resource, in_process_executor, fs_io_manager

# Resource definitions (placeholders)
@resource
def airvisual_api_resource(_):
    """Placeholder resource for AirVisual API."""
    return None

@resource
def postgres_conn_resource(_):
    """Placeholder resource for PostgreSQL connection."""
    return None

# Task definitions (use exactly as provided)

@op(
    name='extract_airvisual_data',
    description='Extract AirVisual Data',
)
def extract_airvisual_data(context):
    """Op: Extract AirVisual Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='validate_airvisual_json',
    description='Validate AirVisual JSON',
)
def validate_airvisual_json(context):
    """Op: Validate AirVisual JSON"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_airvisual_to_postgresql',
    description='Load AirVisual Data to PostgreSQL',
)
def load_airvisual_to_postgresql(context):
    """Op: Load AirVisual Data to PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

# Job definition with sequential wiring
@job(
    name="extract_airvisual_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "airvisual_api": airvisual_api_resource,
        "postgres_conn": postgres_conn_resource,
        "io_manager": fs_io_manager,
    },
)
def extract_airvisual_data_pipeline():
    # Sequential execution: extract -> validate -> load
    extract_airvisual_data() >> validate_airvisual_json() >> load_airvisual_to_postgresql()