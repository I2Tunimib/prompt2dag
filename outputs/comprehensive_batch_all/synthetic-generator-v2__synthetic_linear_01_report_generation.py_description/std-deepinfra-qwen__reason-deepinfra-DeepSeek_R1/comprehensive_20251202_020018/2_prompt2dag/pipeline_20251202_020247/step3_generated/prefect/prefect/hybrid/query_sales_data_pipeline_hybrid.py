from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner

@task(name='query_sales_data', retries=2)
def query_sales_data():
    """Task: Query Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='transform_to_csv', retries=2)
def transform_to_csv():
    """Task: Transform to CSV"""
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

@flow(name="query_sales_data_pipeline", task_runner=SequentialTaskRunner)
def query_sales_data_pipeline():
    logger = get_run_logger()
    logger.info("Starting query_sales_data_pipeline")

    query_sales_data_result = query_sales_data()
    transform_to_csv_result = transform_to_csv(wait_for=[query_sales_data_result])
    generate_pdf_chart_result = generate_pdf_chart(wait_for=[transform_to_csv_result])
    email_sales_report_result = email_sales_report(wait_for=[generate_pdf_chart_result])

    logger.info("query_sales_data_pipeline completed successfully")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=query_sales_data_pipeline,
        name="query_sales_data_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *", timezone="UTC"),
    )
    deployment.apply()