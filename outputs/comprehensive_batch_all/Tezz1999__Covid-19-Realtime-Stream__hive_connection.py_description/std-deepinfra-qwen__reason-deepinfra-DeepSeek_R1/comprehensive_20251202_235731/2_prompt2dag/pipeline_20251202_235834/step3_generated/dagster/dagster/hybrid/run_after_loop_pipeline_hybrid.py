from dagster import job, op, in_process_executor, fs_io_manager, ResourceDefinition

@op(
    name='run_after_loop',
    description='Run After Loop',
)
def run_after_loop(context):
    """Op: Run After Loop"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='hive_script_task',
    description='Hive Script Task',
)
def hive_script_task(context):
    """Op: Hive Script Task"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="run_after_loop_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "hive_local": ResourceDefinition.hardcoded_resource(None)},
)
def run_after_loop_pipeline():
    hive_script_task(run_after_loop())