from dagster import job, op, Out, In, ResourceDefinition, fs_io_manager, multiprocess_executor, daily_schedule

# Task Definitions
@op(
    name='read_csv',
    description='Read CSV',
)
def read_csv(context):
    """Op: Read CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='account_check',
    description='Account Check',
)
def account_check(context):
    """Op: Account Check"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='route_to_fatca',
    description='Route to FATCA',
)
def route_to_fatca(context):
    """Op: Route to FATCA"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='route_to_irs',
    description='Route to IRS',
)
def route_to_irs(context):
    """Op: Route to IRS"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='archive_reports',
    description='Archive Reports',
)
def archive_reports(context):
    """Op: Archive Reports"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="read_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "irs_system": ResourceDefinition.hardcoded_resource(None, "irs_system"),
        "fatca_system": ResourceDefinition.hardcoded_resource(None, "fatca_system"),
        "local_filesystem": ResourceDefinition.hardcoded_resource(None, "local_filesystem"),
        "secure_archive": ResourceDefinition.hardcoded_resource(None, "secure_archive"),
    },
)
def read_csv_pipeline():
    read_csv_output = read_csv()
    account_check_output = account_check(read_csv_output)
    route_to_fatca_output = route_to_fatca(account_check_output)
    route_to_irs_output = route_to_irs(account_check_output)
    archive_reports(route_to_fatca_output, route_to_irs_output)

# Schedule Definition
@daily_schedule(
    pipeline_name="read_csv_pipeline",
    start_date="2023-10-01",
    execution_time="00:00",
    execution_timezone="UTC",
)
def read_csv_pipeline_schedule(context):
    return {}

# Ensure the schedule is enabled
read_csv_pipeline_schedule.is_enabled = True