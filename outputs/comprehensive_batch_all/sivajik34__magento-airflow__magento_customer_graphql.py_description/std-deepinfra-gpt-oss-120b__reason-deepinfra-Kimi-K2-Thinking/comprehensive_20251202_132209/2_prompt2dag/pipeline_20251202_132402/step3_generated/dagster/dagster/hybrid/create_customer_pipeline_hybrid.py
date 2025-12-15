from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition


# Define required resource placeholder
magento_default = ResourceDefinition.hardcoded_resource({})


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


@job(
    name="create_customer_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "magento_default": magento_default,
        "io_manager": fs_io_manager,
    },
)
def create_customer_pipeline():
    # Sequential wiring of ops
    get_customer_info(
        generate_customer_token(
            create_customer()
        )
    )