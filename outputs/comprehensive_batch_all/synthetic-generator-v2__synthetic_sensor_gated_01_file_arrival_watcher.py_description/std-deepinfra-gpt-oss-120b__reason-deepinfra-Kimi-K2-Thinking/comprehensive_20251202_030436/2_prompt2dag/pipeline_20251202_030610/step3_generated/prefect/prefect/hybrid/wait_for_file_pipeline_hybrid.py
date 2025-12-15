from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='wait_for_file', retries=0)
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
    name="wait_for_file_pipeline",
    task_runner=SequentialTaskRunner()
)
def wait_for_file_pipeline():
    """Sequential pipeline that waits for a file, validates its schema, and loads it into PostgreSQL."""
    wait_for_file()
    validate_schema()
    load_db()


if __name__ == "__main__":
    wait_for_file_pipeline()