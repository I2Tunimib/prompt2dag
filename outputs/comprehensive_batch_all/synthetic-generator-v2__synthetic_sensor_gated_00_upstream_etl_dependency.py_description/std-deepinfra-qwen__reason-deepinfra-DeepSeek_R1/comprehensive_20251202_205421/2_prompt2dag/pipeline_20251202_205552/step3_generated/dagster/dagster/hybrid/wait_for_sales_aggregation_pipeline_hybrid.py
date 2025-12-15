from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='wait_for_sales_aggregation',
    description='Wait for Sales Aggregation',
)
def wait_for_sales_aggregation(context):
    """Op: Wait for Sales Aggregation"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_sales_csv',
    description='Load Sales CSV',
)
def load_sales_csv(context):
    """Op: Load Sales CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='generate_dashboard',
    description='Generate Dashboard',
)
def generate_dashboard(context):
    """Op: Generate Dashboard"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="wait_for_sales_aggregation_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "daily_sales_aggregation": ResourceDefinition.hardcoded_resource(None)},
)
def wait_for_sales_aggregation_pipeline():
    generate_dashboard(load_sales_csv(wait_for_sales_aggregation()))