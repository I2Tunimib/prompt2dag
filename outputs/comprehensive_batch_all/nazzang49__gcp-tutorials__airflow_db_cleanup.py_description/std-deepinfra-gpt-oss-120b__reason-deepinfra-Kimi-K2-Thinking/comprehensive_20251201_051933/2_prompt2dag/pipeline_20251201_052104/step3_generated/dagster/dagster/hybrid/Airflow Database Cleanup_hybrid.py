from dagster import (
    op,
    job,
    InProcessExecutor,
    fs_io_manager,
    resource,
    ScheduleDefinition,
    ScheduleStatus,
)


@op(
    name="extract_cleanup_configuration",
    description="Extract Cleanup Configuration",
)
def extract_cleanup_configuration(context):
    """Op: Extract Cleanup Configuration"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="cleanup_airflow_metadb",
    description="Cleanup Airflow MetaDB",
)
def cleanup_airflow_metadb(context):
    """Op: Cleanup Airflow MetaDB"""
    # Docker execution
    # Image: python:3.9
    pass


@resource
def airflow_default_db(_):
    # Placeholder for the actual Airflow MetaDB connection/resource.
    return {}


@job(
    name="airflow_database_cleanup",
    description="Maintenance workflow that periodically cleans old metadata entries from Airflow's MetaStore database tables to prevent excessive data accumulation.",
    executor_def=InProcessExecutor(),
    resource_defs={"airflow_default_db": airflow_default_db},
    io_manager_defs={"io_manager": fs_io_manager},
)
def airflow_database_cleanup():
    extract = extract_cleanup_configuration()
    cleanup = cleanup_airflow_metadb()
    extract >> cleanup


airflow_database_cleanup_schedule = ScheduleDefinition(
    job=airflow_database_cleanup,
    cron_schedule="@daily",
    name="airflow_database_cleanup_schedule",
    description="Daily schedule for Airflow Database Cleanup job.",
    default_status=ScheduleStatus.RUNNING,
)