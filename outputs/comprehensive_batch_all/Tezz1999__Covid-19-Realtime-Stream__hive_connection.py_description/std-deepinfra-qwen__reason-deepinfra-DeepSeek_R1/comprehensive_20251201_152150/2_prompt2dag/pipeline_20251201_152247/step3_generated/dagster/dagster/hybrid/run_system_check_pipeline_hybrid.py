from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

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
    name='execute_hive_script',
    description='Execute Hive Script',
)
def execute_hive_script(context):
    """Op: Execute Hive Script"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="run_system_check_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "hive_local": ResourceDefinition.hardcoded_resource(None)},
)
def run_system_check_pipeline():
    execute_hive_script(run_system_check())