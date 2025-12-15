from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='ingest_csv',
    description='Ingest CSV',
)
def ingest_csv(context):
    """Op: Ingest CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='quality_check',
    description='Quality Check',
)
def quality_check(context):
    """Op: Quality Check"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='production_load',
    description='Production Load',
)
def production_load(context):
    """Op: Production Load"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='quarantine_and_alert',
    description='Quarantine and Alert',
)
def quarantine_and_alert(context):
    """Op: Quarantine and Alert"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_alert_email',
    description='Send Alert Email',
)
def send_alert_email(context):
    """Op: Send Alert Email"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='cleanup',
    description='Cleanup',
)
def cleanup(context):
    """Op: Cleanup"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def quarantine_storage(context):
    pass

@resource
def email_system(context):
    pass

@resource
def file_system(context):
    pass

@resource
def production_db(context):
    pass

# Job Definition
@job(
    name="ingest_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "quarantine_storage": quarantine_storage,
        "email_system": email_system,
        "file_system": file_system,
        "production_db": production_db,
    },
    io_manager_def=fs_io_manager,
)
def ingest_csv_pipeline():
    ingest_csv_output = ingest_csv()
    quality_check_output = quality_check(ingest_csv_output)
    production_load_output = production_load(quality_check_output)
    quarantine_and_alert_output = quarantine_and_alert(quality_check_output)
    send_alert_email_output = send_alert_email(quarantine_and_alert_output)
    cleanup(production_load_output, send_alert_email_output)