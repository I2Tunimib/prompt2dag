from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule


@task(name='wait_for_sales_aggregation', retries=0)
def wait_for_sales_aggregation():
    """Task: Wait for Sales Aggregation DAG"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_sales_csv', retries=2)
def load_sales_csv():
    """Task: Load Aggregated Sales CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_dashboard', retries=2)
def generate_dashboard():
    """Task: Generate Executive Dashboard"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="wait_for_sales_aggregation_pipeline",
    task_runner=SequentialTaskRunner()
)
def wait_for_sales_aggregation_pipeline():
    """Sequential pipeline orchestrating sales aggregation to dashboard generation."""
    wait_for_sales_aggregation()
    load_sales_csv()
    generate_dashboard()


# Deployment configuration
DeploymentSpec(
    name="wait_for_sales_aggregation_pipeline_deployment",
    flow=wait_for_sales_aggregation_pipeline,
    schedule=CronSchedule(cron="0 0 * * *"),  # daily schedule
    work_pool_name="default-agent-pool",
)