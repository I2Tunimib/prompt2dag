from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition


@op(
    name='extract_mahidol_aqi_html',
    description='Extract Mahidol AQI HTML',
)
def extract_mahidol_aqi_html(context):
    """Op: Extract Mahidol AQI HTML"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform_mahidol_aqi_json',
    description='Transform Mahidol AQI to JSON',
)
def transform_mahidol_aqi_json(context):
    """Op: Transform Mahidol AQI to JSON"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='notify_pm25_email',
    description='Notify PM2.5 Email Alert',
)
def notify_pm25_email(context):
    """Op: Notify PM2.5 Email Alert"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_mahidol_aqi_postgres',
    description='Load Mahidol AQI into PostgreSQL',
)
def load_mahidol_aqi_postgres(context):
    """Op: Load Mahidol AQI into PostgreSQL"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='pm2.5_risk_alert_pipeline',
    description='Comprehensive Pipeline Description',
    executor_def=in_process_executor,
    resource_defs={
        "fs_io_manager": fs_io_manager,
        "smtp_gmail": ResourceDefinition.hardcoded_resource(None),
        "postgres_conn": ResourceDefinition.hardcoded_resource(None),
        "http_mahidol_aqi": ResourceDefinition.hardcoded_resource(None),
    },
)
def pm2_5_risk_alert_pipeline():
    # Sequential execution of tasks
    extract_mahidol_aqi_html()
    transform_mahidol_aqi_json()
    load_mahidol_aqi_postgres()
    notify_pm25_email()