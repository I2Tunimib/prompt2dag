from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


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


@flow(name="wait_partition_pipeline", task_runner=SequentialTaskRunner())
def wait_partition_pipeline():
    """Database Partition Check ETL pipeline."""
    # Entry task
    wait_partition()
    # Subsequent tasks respecting dependencies
    extract_incremental()
    transform_orders()
    load_orders()


if __name__ == "__main__":
    wait_partition_pipeline()