from prefect import flow, task, SequentialTaskRunner

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

@flow(name="fetch_user_data_pipeline", task_runner=SequentialTaskRunner)
def fetch_user_data_pipeline():
    fetch_user_data_result = fetch_user_data()
    process_user_data_result = process_user_data(wait_for=[fetch_user_data_result])
    create_user_table_result = create_user_table(wait_for=[process_user_data_result])
    insert_user_data_result = insert_user_data(wait_for=[create_user_table_result])

if __name__ == "__main__":
    fetch_user_data_pipeline()