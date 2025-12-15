from dagster import job, op, in_process_executor, fs_io_manager, daily_schedule

@op(
    name='print_configuration',
    description='Print Configuration',
)
def print_configuration(context):
    """Op: Print Configuration"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='cleanup_airflow_metadb',
    description='Cleanup Airflow MetaDB',
)
def cleanup_airflow_metadb(context):
    """Op: Cleanup Airflow MetaDB"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="print_configuration_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "airflow_metadb": ResourceDefinition.hardcoded_resource(None, "airflow_metadb"),
        "airflow_variables": ResourceDefinition.hardcoded_resource(None, "airflow_variables"),
        "xcom": ResourceDefinition.hardcoded_resource(None, "xcom"),
    },
    op_defs=[print_configuration, cleanup_airflow_metadb],
    tags={"dagster_version": "1.5.0"},
)
def print_configuration_pipeline():
    cleanup_airflow_metadb(print_configuration())

@daily_schedule(
    pipeline_name="print_configuration_pipeline",
    start_date=datetime.datetime(2023, 1, 1),
    execution_time=datetime.time(hour=0, minute=0),
    execution_timezone="UTC",
)
def print_configuration_pipeline_schedule():
    return {}