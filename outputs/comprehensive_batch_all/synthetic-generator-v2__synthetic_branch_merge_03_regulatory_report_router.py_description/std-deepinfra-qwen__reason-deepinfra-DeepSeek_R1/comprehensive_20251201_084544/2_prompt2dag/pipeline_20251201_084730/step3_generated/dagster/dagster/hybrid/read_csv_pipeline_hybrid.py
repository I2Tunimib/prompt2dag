from dagster import job, op, Out, In, graph, multiprocess_executor, fs_io_manager

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
        "secure_archive": None,
        "fatca_system": None,
        "irs_system": None,
        "local_filesystem": None,
    },
    io_manager_def=fs_io_manager,
)
def read_csv_pipeline():
    read_csv_output = read_csv()
    account_check_output = account_check(read_csv_output)
    fatca_output = route_to_fatca(account_check_output)
    irs_output = route_to_irs(account_check_output)
    archive_reports(fatca_output, irs_output)