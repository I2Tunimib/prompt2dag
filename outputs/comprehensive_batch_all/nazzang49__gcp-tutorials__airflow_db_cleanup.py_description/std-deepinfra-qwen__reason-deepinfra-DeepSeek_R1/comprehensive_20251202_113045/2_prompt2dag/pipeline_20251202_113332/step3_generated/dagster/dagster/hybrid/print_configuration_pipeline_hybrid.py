from dagster import job, op, in_process_executor, fs_io_manager, resource, daily_schedule

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

@resource
def airflow_db():
    return None

@job(
    name="print_configuration_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"airflow_db": airflow_db},
    io_manager_def=fs_io_manager,
)
def print_configuration_pipeline():
    cleanup_airflow_metadb(print_configuration())

@daily_schedule(
    pipeline_name="print_configuration_pipeline",
    start_date='2023-01-01',
    execution_time='00:00',
    execution_timezone='UTC',
)
def print_configuration_pipeline_schedule():
    return {}