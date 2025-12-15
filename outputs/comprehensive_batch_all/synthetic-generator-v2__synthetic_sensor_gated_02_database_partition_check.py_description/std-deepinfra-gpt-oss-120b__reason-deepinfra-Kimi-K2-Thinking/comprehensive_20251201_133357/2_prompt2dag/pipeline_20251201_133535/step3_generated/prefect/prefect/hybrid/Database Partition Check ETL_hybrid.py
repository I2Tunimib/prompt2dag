from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule


@task(name='wait_partition', retries=2)
def wait_partition():
    """Task: Wait for Daily Partition"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='extract_incremental', retries=2)
def extract_incremental():
    """Task: Extract Incremental Orders"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_orders', retries=2)
def transform_orders():
    """Task: Transform Orders Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_orders', retries=2)
def load_orders():
    """Task: Load Orders to Warehouse"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="database_partition_check_etl", task_runner=SequentialTaskRunner())
def database_partition_check_etl():
    """Sensor‑gated daily ETL pipeline that waits for a database partition before processing orders."""
    wait_partition()
    extract_incremental()
    transform_orders()
    load_orders()


# Deployment configuration
DeploymentSpec(
    name="database_partition_check_etl_deployment",
    flow=database_partition_check_etl,
    schedule=CronSchedule(cron="0 0 * * *"),  # daily at midnight
    work_pool_name="default-agent-pool",
)