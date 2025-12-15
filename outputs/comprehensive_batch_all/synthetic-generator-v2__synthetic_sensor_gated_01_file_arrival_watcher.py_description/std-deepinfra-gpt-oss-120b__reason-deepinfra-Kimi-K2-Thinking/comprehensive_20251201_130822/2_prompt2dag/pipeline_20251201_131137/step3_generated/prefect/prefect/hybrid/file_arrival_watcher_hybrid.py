from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="wait_for_file", retries=2)
def wait_for_file():
    """Task: Wait for Transaction File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="validate_schema", retries=2)
def validate_schema():
    """Task: Validate Transaction File Schema"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="load_db", retries=2)
def load_db():
    """Task: Load Validated Transactions to PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="file_arrival_watcher",
    task_runner=SequentialTaskRunner(),
)
def file_arrival_watcher():
    """Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL."""
    wait_for_file()
    validate_schema()
    load_db()


if __name__ == "__main__":
    file_arrival_watcher()