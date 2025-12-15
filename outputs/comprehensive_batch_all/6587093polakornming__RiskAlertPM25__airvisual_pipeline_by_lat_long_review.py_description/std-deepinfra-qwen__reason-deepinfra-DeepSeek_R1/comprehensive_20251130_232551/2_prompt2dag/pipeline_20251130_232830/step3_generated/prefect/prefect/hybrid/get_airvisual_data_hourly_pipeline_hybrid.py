from prefect import flow, task, SequentialTaskRunner

@task(name='get_airvisual_data_hourly', retries=2)
def get_airvisual_data_hourly():
    """Task: Fetch AirVisual Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='read_data_airvisual', retries=2)
def read_data_airvisual():
    """Task: Read and Validate AirVisual Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_data_airvisual_to_postgresql', retries=2)
def load_data_airvisual_to_postgresql():
    """Task: Load AirVisual Data to PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="get_airvisual_data_hourly_pipeline", task_runner=SequentialTaskRunner)
def get_airvisual_data_hourly_pipeline():
    get_airvisual_data_hourly_result = get_airvisual_data_hourly()
    read_data_airvisual_result = read_data_airvisual(wait_for=[get_airvisual_data_hourly_result])
    load_data_airvisual_to_postgresql_result = load_data_airvisual_to_postgresql(wait_for=[read_data_airvisual_result])

if __name__ == "__main__":
    get_airvisual_data_hourly_pipeline()