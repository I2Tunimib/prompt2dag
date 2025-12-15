from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition

@op(
    name='extract_claims',
    description='Extract Claims CSV',
)
def extract_claims(context):
    """Op: Extract Claims CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='extract_providers',
    description='Extract Providers CSV',
)
def extract_providers(context):
    """Op: Extract Providers CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform_join',
    description='Join and Anonymize Claims with Providers',
)
def transform_join(context):
    """Op: Join and Anonymize Claims with Providers"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_warehouse',
    description='Load Transformed Data to Data Warehouse',
)
def load_warehouse(context):
    """Op: Load Transformed Data to Data Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='refresh_bi',
    description='Refresh BI Dashboards',
)
def refresh_bi(context):
    """Op: Refresh BI Dashboards"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="healthcare_claims_etl",
    description="Comprehensive Pipeline Description",
    executor_def=multiprocess_executor,
    resource_defs={
        "power_bi_api": ResourceDefinition.hardcoded_resource(None),
        "tableau_api": ResourceDefinition.hardcoded_resource(None),
        "local_fs": ResourceDefinition.hardcoded_resource(None),
        "postgres_warehouse": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def healthcare_claims_etl():
    # Entry point ops
    claims = extract_claims()
    providers = extract_providers()

    # Fan‑out / fan‑in join
    joined = transform_join()
    claims >> joined
    providers >> joined

    # Downstream ops depending on the join
    load_node = load_warehouse()
    refresh_node = refresh_bi()
    joined >> load_node
    joined >> refresh_node