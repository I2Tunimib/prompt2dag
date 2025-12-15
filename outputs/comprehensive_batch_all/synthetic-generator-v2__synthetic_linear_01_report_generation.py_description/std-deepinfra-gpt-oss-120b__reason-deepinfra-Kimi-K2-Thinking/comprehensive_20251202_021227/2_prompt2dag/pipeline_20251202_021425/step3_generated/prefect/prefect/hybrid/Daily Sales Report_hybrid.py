from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='query_sales_data', retries=2)
def query_sales_data():
    """Task: Query Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_to_csv', retries=2)
def transform_to_csv():
    """Task: Transform Sales Data to CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_pdf_chart', retries=2)
def generate_pdf_chart():
    """Task: Generate PDF Chart"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='email_sales_report', retries=2)
def email_sales_report():
    """Task: Email Sales Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="daily_sales_report", task_runner=SequentialTaskRunner())
def daily_sales_report():
    """Sequential pipeline for daily sales reporting."""
    query_sales_data()
    transform_to_csv()
    generate_pdf_chart()
    email_sales_report()


if __name__ == "__main__":
    daily_sales_report()