from dagster import (
    op,
    job,
    in_process_executor,
    fs_io_manager,
    ResourceDefinition,
    schedule,
)


# Resource placeholders
airflow_variables = ResourceDefinition.hardcoded_resource({})
airflow_metastore = ResourceDefinition.hardcoded_resource({})
xcom = ResourceDefinition.hardcoded_resource({})


@op(
    name="load_cleanup_configuration",
    description="Load Cleanup Configuration",
)
def load_cleanup_configuration(context):
    """Op: Load Cleanup Configuration"""
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


@job(
    name="airflow_database_cleanup",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "airflow_variables": airflow_variables,
        "airflow_metastore": airflow_metastore,
        "xcom": xcom,
        "io_manager": fs_io_manager,
    },
)
def airflow_database_cleanup():
    config = load_cleanup_configuration()
    cleanup_airflow_metadb(config)


@schedule(
    cron_schedule="@daily",
    job=airflow_database_cleanup,
    execution_timezone="UTC",
)
def airflow_database_cleanup_schedule():
    return {}