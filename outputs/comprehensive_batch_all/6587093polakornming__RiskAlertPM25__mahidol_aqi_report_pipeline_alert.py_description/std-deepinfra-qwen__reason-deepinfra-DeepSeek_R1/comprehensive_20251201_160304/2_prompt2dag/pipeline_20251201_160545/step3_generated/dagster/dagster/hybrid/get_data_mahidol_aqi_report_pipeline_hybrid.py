from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='get_data_mahidol_aqi_report',
    description='Get Mahidol AQI Report',
)
def get_data_mahidol_aqi_report(context):
    """Op: Get Mahidol AQI Report"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='create_json_object',
    description='Create JSON Object',
)
def create_json_object(context):
    """Op: Create JSON Object"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_mahidol_aqi_to_postgres',
    description='Load Mahidol AQI to PostgreSQL',
)
def load_mahidol_aqi_to_postgres(context):
    """Op: Load Mahidol AQI to PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='alert_email',
    description='Send Email Alert',
)
def alert_email(context):
    """Op: Send Email Alert"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="get_data_mahidol_aqi_report_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "smtp_conn": ResourceDefinition.mock_resource(),
        "http_mahidol": ResourceDefinition.mock_resource(),
        "postgres_conn": ResourceDefinition.mock_resource(),
    },
)
def get_data_mahidol_aqi_report_pipeline():
    data = get_data_mahidol_aqi_report()
    json_data = create_json_object(data)
    loaded_data = load_mahidol_aqi_to_postgres(json_data)
    alert_email(loaded_data)