from prefect import flow, task, get_run_logger
from prefect.orion.schemas.states import Completed
from prefect.task_runners import SequentialTaskRunner

@task(name='fetch_user_data', retries=0)
def fetch_user_data():
    """Task: Fetch User Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='process_user_data', retries=0)
def process_user_data():
    """Task: Process User Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='create_user_table', retries=0)
def create_user_table():
    """Task: Create User Table"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='insert_user_data', retries=0)
def insert_user_data():
    """Task: Insert User Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="fetch_user_data_pipeline", task_runner=SequentialTaskRunner())
def fetch_user_data_pipeline():
    logger = get_run_logger()
    logger.info("Starting fetch_user_data_pipeline")

    fetch_data = fetch_user_data.submit()
    process_data = process_user_data.submit(wait_for=[fetch_data])
    create_table = create_user_table.submit(wait_for=[process_data])
    insert_data = insert_user_data.submit(wait_for=[create_table])

    logger.info("fetch_user_data_pipeline completed successfully")

if __name__ == "__main__":
    fetch_user_data_pipeline()