from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition, hardcoded_resource

# Pre-generated task definitions (use exactly as provided)

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


# Define required resources
hive_local_resource = hardcoded_resource(None)  # Placeholder for the actual Hive resource


@job(
    name="run_system_check_pipeline",
    description="This is a simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data. The pipeline follows a sequential topology pattern with two tasks executing in strict order. Key infrastructure features include Hive database connectivity and scheduled daily execution at 1:00 AM.",
    executor_def=in_process_executor,
    resource_defs={
        "hive_local": hive_local_resource,
        "io_manager": fs_io_manager,
    },
)
def run_system_check_pipeline():
    # Sequential wiring: run_system_check -> execute_hive_script
    execute_hive_script(run_system_check())