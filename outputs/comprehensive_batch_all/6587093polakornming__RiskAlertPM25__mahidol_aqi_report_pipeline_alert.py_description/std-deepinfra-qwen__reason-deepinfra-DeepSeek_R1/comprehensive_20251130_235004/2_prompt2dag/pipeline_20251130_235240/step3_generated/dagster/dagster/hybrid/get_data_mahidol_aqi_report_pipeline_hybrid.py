from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='get_data_mahidol_aqi_report',
    description='Scrape Mahidol AQI Report',
)
def get_data_mahidol_aqi_report(context):
    """Op: Scrape Mahidol AQI Report"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='create_json_object',
    description='Parse and Validate AQI Data',
)
def create_json_object(context):
    """Op: Parse and Validate AQI Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_mahidol_aqi_to_postgres',
    description='Load AQI Data to PostgreSQL',
)
def load_mahidol_aqi_to_postgres(context):
    """Op: Load AQI Data to PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='alert_email',
    description='Send AQI Alert Emails',
)
def alert_email(context):
    """Op: Send AQI Alert Emails"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="get_data_mahidol_aqi_report_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "postgres_conn": ResourceDefinition.hardcoded_resource(None, "postgres_conn"),
        "smtp_conn": ResourceDefinition.hardcoded_resource(None, "smtp_conn"),
        "mahidol_aqi_website": ResourceDefinition.hardcoded_resource(None, "mahidol_aqi_website"),
    },
)
def get_data_mahidol_aqi_report_pipeline():
    aqi_data = get_data_mahidol_aqi_report()
    parsed_data = create_json_object(aqi_data)
    loaded_data = load_mahidol_aqi_to_postgres(parsed_data)
    alert_email(loaded_data)