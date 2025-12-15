from prefect import flow, task, SequentialTaskRunner
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

@task(name='wait_for_sales_aggregation', retries=0)
def wait_for_sales_aggregation():
    """Task: Wait for Sales Aggregation"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_sales_csv', retries=2)
def load_sales_csv():
    """Task: Load Sales CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='generate_dashboard', retries=2)
def generate_dashboard():
    """Task: Generate Dashboard"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="wait_for_sales_aggregation_pipeline", task_runner=SequentialTaskRunner)
def wait_for_sales_aggregation_pipeline():
    wait_for_sales_aggregation_result = wait_for_sales_aggregation()
    load_sales_csv_result = load_sales_csv(wait_for_sales_aggregation_result)
    generate_dashboard_result = generate_dashboard(load_sales_csv_result)

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=wait_for_sales_aggregation_pipeline,
        name="wait_for_sales_aggregation_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *", timezone="UTC"),
    )
    deployment.apply()