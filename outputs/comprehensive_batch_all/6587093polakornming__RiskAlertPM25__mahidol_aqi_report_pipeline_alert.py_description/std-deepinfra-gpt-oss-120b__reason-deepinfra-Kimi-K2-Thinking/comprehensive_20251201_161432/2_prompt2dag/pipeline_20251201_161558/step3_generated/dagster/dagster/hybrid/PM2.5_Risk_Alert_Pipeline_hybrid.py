from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition

# ----------------------------------------------------------------------
# Pre-generated task definitions (used exactly as provided)
# ----------------------------------------------------------------------
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
    name='transform_html_to_json',
    description='Transform HTML to Structured JSON',
)
def transform_html_to_json(context):
    """Op: Transform HTML to Structured JSON"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_mahidol_aqi_to_warehouse',
    description='Load AQI Data to PostgreSQL Warehouse',
)
def load_mahidol_aqi_to_warehouse(context):
    """Op: Load AQI Data to PostgreSQL Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='notify_pm25_alert',
    description='PM2.5 Email Alert Notification',
)
def notify_pm25_alert(context):
    """Op: PM2.5 Email Alert Notification"""
    # Docker execution
    # Image: python:3.9
    pass

# ----------------------------------------------------------------------
# Additional placeholder ops to satisfy declared dependencies
# ----------------------------------------------------------------------
@op(
    name='duplicate_check_branch',
    description='Duplicate check branch (placeholder)',
)
def duplicate_check_branch(context):
    """Placeholder op for duplicate check branching logic."""
    pass


@op(
    name='aqi_threshold_branch',
    description='AQI threshold branch (placeholder)',
)
def aqi_threshold_branch(context):
    """Placeholder op for AQI threshold branching logic."""
    pass

# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------
@job(
    name='pm2.5_risk_alert_pipeline',
    description='Sequential ETL pipeline that scrapes Mahidol University AQI data, transforms it to JSON, loads it into PostgreSQL, and sends email alerts when PM2.5 exceeds thresholds.',
    executor_def=in_process_executor,
    resource_defs={
        "postgres_conn": ResourceDefinition.hardcoded_resource(None),
        "filesystem_local": ResourceDefinition.hardcoded_resource(None),
        "smtp_gmail": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def pm2_5_risk_alert_pipeline():
    # Sequential wiring of ops respecting the declared dependencies
    extract_mahidol_aqi_html() \
        >> transform_html_to_json() \
        >> duplicate_check_branch() \
        >> load_mahidol_aqi_to_warehouse() \
        >> aqi_threshold_branch() \
        >> notify_pm25_alert()