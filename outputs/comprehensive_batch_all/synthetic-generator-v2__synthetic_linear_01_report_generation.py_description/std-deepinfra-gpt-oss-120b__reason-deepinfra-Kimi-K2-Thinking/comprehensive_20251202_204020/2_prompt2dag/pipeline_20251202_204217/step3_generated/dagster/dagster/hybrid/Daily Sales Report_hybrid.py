from dagster import op, job, in_process_executor, fs_io_manager, resource


@resource
def local_filesystem(_):
    """Placeholder for a local filesystem resource."""
    return None


@resource
def postgres_default(_):
    """Placeholder for a PostgreSQL connection resource."""
    return None


@resource
def email_smtp(_):
    """Placeholder for an SMTP email resource."""
    return None


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


@job(
    name='daily_sales_report',
    description='Generates daily sales reports by querying PostgreSQL, converting the result to CSV, creating a PDF chart, and emailing the final report to management.',
    executor_def=in_process_executor,
    resource_defs={
        "local_filesystem": local_filesystem,
        "postgres_default": postgres_default,
        "email_smtp": email_smtp,
    },
    io_manager_defs={"io_manager": fs_io_manager},
)
def daily_sales_report():
    # Sequential execution: query -> csv -> pdf -> email
    query_sales_data() \
        >> transform_to_csv() \
        >> generate_pdf_chart() \
        >> email_sales_report()