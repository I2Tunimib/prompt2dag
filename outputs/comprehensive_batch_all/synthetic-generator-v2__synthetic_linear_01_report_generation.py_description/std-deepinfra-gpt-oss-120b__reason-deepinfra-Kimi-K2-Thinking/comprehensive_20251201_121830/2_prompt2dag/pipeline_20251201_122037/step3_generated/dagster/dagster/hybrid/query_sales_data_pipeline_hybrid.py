from dagster import op, job, in_process_executor, fs_io_manager, resource

# Resource definitions (placeholders)
@resource
def local_fs():
    """Placeholder for local filesystem resource."""
    return {}

@resource
def email_smtp():
    """Placeholder for SMTP email resource."""
    return {}

@resource
def postgres_default():
    """Placeholder for PostgreSQL connection resource."""
    return {}

# Pre‑generated task definitions (use exactly as provided)

@op(
    name='query_sales_data',
    description='Query Sales Data',
)
def query_sales_data(context):
    """Op: Query Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform_to_csv',
    description='Transform Sales Data to CSV',
)
def transform_to_csv(context):
    """Op: Transform Sales Data to CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='generate_pdf_chart',
    description='Generate PDF Chart',
)
def generate_pdf_chart(context):
    """Op: Generate PDF Chart"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='email_sales_report',
    description='Email Sales Report',
)
def email_sales_report(context):
    """Op: Email Sales Report"""
    # Docker execution
    # Image: python:3.9
    pass


# Job definition with sequential wiring
@job(
    name="query_sales_data_pipeline",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL sales data, converting it to CSV, creating a PDF chart, and emailing the report to management.",
    executor_def=in_process_executor,
    resource_defs={
        "local_fs": local_fs,
        "email_smtp": email_smtp,
        "postgres_default": postgres_default,
        "io_manager": fs_io_manager,
    },
)
def query_sales_data_pipeline():
    # Instantiate ops
    query = query_sales_data()
    csv = transform_to_csv()
    pdf = generate_pdf_chart()
    email = email_sales_report()

    # Define sequential dependencies
    query >> csv >> pdf >> email