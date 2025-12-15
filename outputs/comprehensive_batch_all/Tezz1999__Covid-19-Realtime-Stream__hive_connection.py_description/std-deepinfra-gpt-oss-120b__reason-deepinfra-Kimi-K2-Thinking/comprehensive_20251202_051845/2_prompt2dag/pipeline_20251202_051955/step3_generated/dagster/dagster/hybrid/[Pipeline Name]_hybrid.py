import dagster
from dagster import op, job, in_process_executor, fs_io_manager, resource

@resource
def hive_local(_):
    """Placeholder resource for Hive interactions."""
    return None

@op(
    name='run_system_check',
    description='Run System Check',
)
def run_system_check(context):
    """Op: Run System Check"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='run_hive_script',
    description='Run Hive Script',
)
def run_hive_script(context):
    """Op: Run Hive Script"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="pipeline_name",
    description="Simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data.",
    executor_def=in_process_executor,
    resource_defs={"hive_local": hive_local, "io_manager": fs_io_manager},
)
def pipeline_name():
    run_system_check() >> run_hive_script()