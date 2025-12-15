from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

@task(name='fetch_user_data', retries=0)
def fetch_user_data():
    """Task: Fetch User Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='transform_user_data', retries=0)
def transform_user_data():
    """Task: Transform User Data"""
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
    # Entry point
    fetch_user_data()
    # Dependent tasks
    transform_user_data()
    create_user_table()
    insert_user_data()

if __name__ == "__main__":
    fetch_user_data_pipeline()