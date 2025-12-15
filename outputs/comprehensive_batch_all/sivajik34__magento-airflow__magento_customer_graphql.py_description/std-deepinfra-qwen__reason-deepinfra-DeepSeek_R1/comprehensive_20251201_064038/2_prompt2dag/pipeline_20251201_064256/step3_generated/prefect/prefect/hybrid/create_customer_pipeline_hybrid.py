from prefect import flow, task, SequentialTaskRunner

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

@flow(name="create_customer_pipeline", task_runner=SequentialTaskRunner)
def create_customer_pipeline():
    create_customer_result = create_customer()
    generate_customer_token_result = generate_customer_token(wait_for=[create_customer_result])
    get_customer_info_result = get_customer_info(wait_for=[generate_customer_token_result])

if __name__ == "__main__":
    create_customer_pipeline()