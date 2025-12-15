from dagster import job, op, Out, In, ResourceDefinition, fs_io_manager, multiprocess_executor, daily_schedule

# Task Definitions
@op(
    name='dump_prod_csv',
    description='Dump Production CSV',
)
def dump_prod_csv(context):
    """Op: Dump Production CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='copy_dev',
    description='Copy to Development',
)
def copy_dev(context):
    """Op: Copy to Development"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='copy_qa',
    description='Copy to QA',
)
def copy_qa(context):
    """Op: Copy to QA"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='copy_staging',
    description='Copy to Staging',
)
def copy_staging(context):
    """Op: Copy to Staging"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="dump_prod_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "qa_db": ResourceDefinition.hardcoded_resource(None, "qa_db"),
        "dev_db": ResourceDefinition.hardcoded_resource(None, "dev_db"),
        "staging_db": ResourceDefinition.hardcoded_resource(None, "staging_db"),
        "local_filesystem": ResourceDefinition.hardcoded_resource(None, "local_filesystem")
    }
)
def dump_prod_csv_pipeline():
    dump_prod_csv_output = dump_prod_csv()
    copy_dev(dump_prod_csv_output)
    copy_qa(dump_prod_csv_output)
    copy_staging(dump_prod_csv_output)

# Schedule Definition
@daily_schedule(
    pipeline_name="dump_prod_csv_pipeline",
    start_date="2023-01-01",
    execution_time_of_day="00:00:00",
    execution_timezone="UTC",
)
def daily_dump_prod_csv_pipeline_schedule(context):
    return {}

# Ensure the schedule is enabled
daily_dump_prod_csv_pipeline_schedule = daily_dump_prod_csv_pipeline_schedule.with_status("RUNNING")