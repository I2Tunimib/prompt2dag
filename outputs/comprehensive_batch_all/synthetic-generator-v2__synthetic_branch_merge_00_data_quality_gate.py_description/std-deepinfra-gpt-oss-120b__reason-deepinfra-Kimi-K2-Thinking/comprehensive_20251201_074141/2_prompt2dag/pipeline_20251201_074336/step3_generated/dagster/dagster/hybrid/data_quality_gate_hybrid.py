from dagster import op, job, multiprocess_executor, fs_io_manager, hardcoded_resource

# -------------------------------------------------
# Task Definitions (use exactly as provided)
# -------------------------------------------------

@op(
    name='ingest_csv',
    description='Ingest Customer CSV',
)
def ingest_csv(context):
    """Op: Ingest Customer CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='quality_check',
    description='Assess Data Quality',
)
def quality_check(context):
    """Op: Assess Data Quality"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='production_load',
    description='Load High-Quality Data to Production',
)
def production_load(context):
    """Op: Load High-Quality Data to Production"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='quarantine_and_alert',
    description='Quarantine Low-Quality Data',
)
def quarantine_and_alert(context):
    """Op: Quarantine Low-Quality Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='send_alert_email',
    description='Send Quality Alert Email',
)
def send_alert_email(context):
    """Op: Send Quality Alert Email"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='cleanup',
    description='Cleanup Temporary Resources',
)
def cleanup(context):
    """Op: Cleanup Temporary Resources"""
    # Docker execution
    # Image: python:3.9
    pass


# -------------------------------------------------
# Job Definition
# -------------------------------------------------

@job(
    name="data_quality_gate",
    description="Implements a data quality gate for customer CSV data that ingests raw data, performs quality assessment, and conditionally routes to production or quarantine based on quality scores.",
    executor_def=multiprocess_executor,
    resource_defs={
        "quarantine_storage": hardcoded_resource(None),
        "fs_temp": hardcoded_resource(None),
        "email_smtp": hardcoded_resource(None),
        "fs_raw": hardcoded_resource(None),
        "prod_db": hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def data_quality_gate():
    # Entry point
    ingest = ingest_csv()
    # Fan‑out from quality check
    qc = quality_check()
    prod = production_load()
    quarantine = quarantine_and_alert()
    alert = send_alert_email()
    # Fan‑in cleanup
    clean = cleanup()

    # Define dependencies (fanout/fanin pattern)
    ingest >> qc
    qc >> prod
    qc >> quarantine
    quarantine >> alert
    prod >> clean
    alert >> clean

# End of file