from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='create_customer', retries=1)
def create_customer():
    """Task: Create Customer"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_customer_token', retries=1)
def generate_customer_token():
    """Task: Generate Customer Token"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='get_customer_info', retries=1)
def get_customer_info():
    """Task: Get Customer Info"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="create_customer_pipeline", task_runner=SequentialTaskRunner())
def create_customer_pipeline():
    """Sequential pipeline to create a customer, generate a token, and retrieve info."""
    create_cust = create_customer()
    token = generate_customer_token(wait_for=[create_cust])
    info = get_customer_info(wait_for=[token])
    return info


if __name__ == "__main__":
    create_customer_pipeline()