from dagster import job, op, multiprocess_executor, fs_io_manager, resource

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

@job(
    name="extract_claims_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "local_filesystem": resource(config_schema={"base_dir": str}),
        "data_warehouse": resource(config_schema={"connection_string": str}),
    },
)
def extract_claims_pipeline():
    claims = extract_claims()
    providers = extract_providers()
    joined_data = transform_join(start=[claims, providers])
    load_warehouse(joined_data)
    refresh_bi(joined_data)