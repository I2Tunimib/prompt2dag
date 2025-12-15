from dagster import job, op, in_process_executor, fs_io_manager, repository, schedule

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
    description='Transform to CSV',
)
def transform_to_csv(context):
    """Op: Transform to CSV"""
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
    name="query_sales_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager},
    required_resource_keys={"postgres_default"},
)
def query_sales_data_pipeline():
    query_sales_data_output = query_sales_data()
    transform_to_csv_output = transform_to_csv(query_sales_data_output)
    generate_pdf_chart_output = generate_pdf_chart(transform_to_csv_output)
    email_sales_report(generate_pdf_chart_output)

@schedule(
    job=query_sales_data_pipeline,
    cron_schedule="@daily",
)
def daily_query_sales_data_pipeline_schedule(context):
    return {}

@repository
def my_repository():
    return [query_sales_data_pipeline, daily_query_sales_data_pipeline_schedule]