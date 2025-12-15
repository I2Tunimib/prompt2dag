from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='extract_claims',
    description='Extract Claims',
)
def extract_claims(context):
    """Op: Extract Claims"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_providers',
    description='Extract Providers',
)
def extract_providers(context):
    """Op: Extract Providers"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='transform_join',
    description='Transform and Join',
)
def transform_join(context):
    """Op: Transform and Join"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_warehouse',
    description='Load Warehouse',
)
def load_warehouse(context):
    """Op: Load Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='refresh_bi',
    description='Refresh BI Tools',
)
def refresh_bi(context):
    """Op: Refresh BI Tools"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def tableau(context):
    pass

@resource
def power_bi(context):
    pass

@resource
def local_filesystem(context):
    pass

@resource
def data_warehouse(context):
    pass

# Job Definition
@job(
    name='extract_claims_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'tableau': tableau,
        'power_bi': power_bi,
        'local_filesystem': local_filesystem,
        'data_warehouse': data_warehouse,
    },
    io_manager_def=fs_io_manager,
)
def extract_claims_pipeline():
    claims = extract_claims()
    providers = extract_providers()
    joined_data = transform_join(start=[claims, providers])
    load_warehouse(joined_data)
    refresh_bi(joined_data)