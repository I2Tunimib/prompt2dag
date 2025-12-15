from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='wait_partition', retries=2)
def wait_partition():
    """Task: Wait for Daily Orders Partition"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='extract_incremental', retries=2)
def extract_incremental():
    """Task: Extract Incremental Orders"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform', retries=2)
def transform():
    """Task: Transform Orders Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load', retries=2)
def load():
    """Task: Load Orders to Data Warehouse"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="wait_partition_pipeline", task_runner=SequentialTaskRunner())
def wait_partition_pipeline():
    """Sequential pipeline orchestrating order processing."""
    wait_partition()
    extract_incremental()
    transform()
    load()


if __name__ == "__main__":
    wait_partition_pipeline()