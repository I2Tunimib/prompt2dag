from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

# Task Definitions
@op(
    name='create_customer',
    description='Create Customer',
)
def create_customer(context):
    """Op: Create Customer"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='generate_customer_token',
    description='Generate Customer Token',
)
def generate_customer_token(context):
    """Op: Generate Customer Token"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='get_customer_info',
    description='Get Customer Info',
)
def get_customer_info(context):
    """Op: Get Customer Info"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="create_customer_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "magento_default": ResourceDefinition.hardcoded_resource("magento_default")},
)
def create_customer_pipeline():
    # Sequential pattern
    customer = create_customer()
    token = generate_customer_token(customer)
    get_customer_info(token)