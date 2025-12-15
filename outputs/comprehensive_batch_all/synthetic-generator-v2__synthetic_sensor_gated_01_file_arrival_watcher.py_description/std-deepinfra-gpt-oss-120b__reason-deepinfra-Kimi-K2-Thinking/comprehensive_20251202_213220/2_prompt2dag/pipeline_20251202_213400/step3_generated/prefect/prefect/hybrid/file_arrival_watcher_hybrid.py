from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='wait_for_file', retries=2)
def wait_for_file():
    """Task: Wait for Transaction File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='validate_schema', retries=2)
def validate_schema():
    """Task: Validate Transaction File Schema"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_db', retries=2)
def load_db():
    """Task: Load Transactions into PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data to PostgreSQL.",
    task_runner=SequentialTaskRunner(),
)
def file_arrival_watcher():
    """Sequential pipeline for processing transaction files."""
    file_ready = wait_for_file()
    schema_ok = validate_schema(wait_for_file=file_ready)
    load_db(validate_schema=schema_ok)


if __name__ == "__main__":
    file_arrival_watcher()