from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='extract_claims',
    description='Extract Claims Data',
)
def extract_claims(context):
    """Op: Extract Claims Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='extract_providers',
    description='Extract Providers Data',
)
def extract_providers(context):
    """Op: Extract Providers Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='transform_join',
    description='Transform and Join Data',
)
def transform_join(context):
    """Op: Transform and Join Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_warehouse',
    description='Load Data to Warehouse',
)
def load_warehouse(context):
    """Op: Load Data to Warehouse"""
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
def data_warehouse():
    pass

@resource
def tableau():
    pass

@resource
def local_filesystem():
    pass

@resource
def power_bi():
    pass

# Job Definition
@job(
    name="extract_claims_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "data_warehouse": data_warehouse,
        "tableau": tableau,
        "local_filesystem": local_filesystem,
        "power_bi": power_bi,
        "io_manager": fs_io_manager,
    },
)
def extract_claims_pipeline():
    claims_data = extract_claims()
    providers_data = extract_providers()
    joined_data = transform_join(claims_data, providers_data)
    load_warehouse(joined_data)
    refresh_bi(joined_data)