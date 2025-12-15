from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition


@op(
    name='wait_for_sales_aggregation',
    description='Wait for Sales Aggregation DAG',
)
def wait_for_sales_aggregation(context):
    """Op: Wait for Sales Aggregation DAG"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_sales_csv',
    description='Load Aggregated Sales CSV',
)
def load_sales_csv(context):
    """Op: Load Aggregated Sales CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='generate_dashboard',
    description='Generate Executive Dashboard',
)
def generate_dashboard(context):
    """Op: Generate Executive Dashboard"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='wait_for_sales_aggregation_pipeline',
    description='No description provided.',
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "fs_sales_data": ResourceDefinition(lambda _: None),
        "fs_reports": ResourceDefinition(lambda _: None),
    },
)
def wait_for_sales_aggregation_pipeline():
    agg = wait_for_sales_aggregation()
    csv = load_sales_csv(agg)
    generate_dashboard(csv)