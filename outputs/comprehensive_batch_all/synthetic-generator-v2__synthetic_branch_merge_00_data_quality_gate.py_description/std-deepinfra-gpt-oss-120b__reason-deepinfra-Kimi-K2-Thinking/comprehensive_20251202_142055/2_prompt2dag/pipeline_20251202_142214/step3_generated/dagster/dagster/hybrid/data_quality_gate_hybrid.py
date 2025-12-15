from dagster import (
    op,
    job,
    ResourceDefinition,
    multiprocess_executor,
    fs_io_manager,
)

# ----------------------------------------------------------------------
# Task Definitions (use exactly as provided)
# ----------------------------------------------------------------------


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
    name='quarantine_and_alert',
    description='Quarantine Low‑Quality Data',
)
def quarantine_and_alert(context):
    """Op: Quarantine Low‑Quality Data"""
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
    description='Final Cleanup',
)
def cleanup(context):
    """Op: Final Cleanup"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='production_load',
    description='Load High‑Quality Data to Production',
)
def production_load(context):
    """Op: Load High‑Quality Data to Production"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Job Definition with fanout_fanin pattern
# ----------------------------------------------------------------------


@job(
    name="data_quality_gate",
    description="Comprehensive Pipeline Description",
    executor_def=multiprocess_executor,
    resource_defs={
        "smtp_email": ResourceDefinition.hardcoded_resource(None),
        "quarantine_storage": ResourceDefinition.hardcoded_resource(None),
        "fs_cleanup": ResourceDefinition.hardcoded_resource(None),
        "fs_raw_data": ResourceDefinition.hardcoded_resource(None),
        "prod_db": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def data_quality_gate():
    # Entry point
    ingest = ingest_csv()
    # Fan‑out from ingest to quality check
    quality = quality_check()
    ingest >> quality

    # Fan‑out from quality check to two parallel branches
    prod = production_load()
    quarantine = quarantine_and_alert()
    quality >> prod
    quality >> quarantine

    # Downstream of quarantine branch
    alert = send_alert_email()
    quarantine >> alert

    # Fan‑in to final cleanup
    cleanup_op = cleanup()
    prod >> cleanup_op
    alert >> cleanup_op