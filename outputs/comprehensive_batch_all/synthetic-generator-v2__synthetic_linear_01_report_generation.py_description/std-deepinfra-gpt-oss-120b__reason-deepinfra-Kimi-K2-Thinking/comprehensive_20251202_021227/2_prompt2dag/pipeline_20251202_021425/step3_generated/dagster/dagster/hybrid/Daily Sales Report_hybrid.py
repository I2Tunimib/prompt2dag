from dagster import op, job, in_process_executor, fs_io_manager, resource


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


@resource
def postgres_default(_):
    """Placeholder resource for PostgreSQL connection."""
    return None


@resource
def email_smtp(_):
    """Placeholder resource for SMTP email sending."""
    return None


@resource
def local_fs(_):
    """Placeholder resource for local filesystem interactions."""
    return None


@job(
    name="daily_sales_report",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL, converting to CSV, creating a PDF chart, and emailing the report.",
    executor_def=in_process_executor,
    resource_defs={
        "postgres_default": postgres_default,
        "email_smtp": email_smtp,
        "local_fs": local_fs,
        "io_manager": fs_io_manager,
    },
)
def daily_sales_report():
    query_sales_data() \
        .then(transform_to_csv()) \
        .then(generate_pdf_chart()) \
        .then(email_sales_report())