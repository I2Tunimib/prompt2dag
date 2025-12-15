from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='extract_airvisual_data', retries=2)
def extract_airvisual_data():
    """Task: Extract AirVisual Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='validate_airvisual_json', retries=2)
def validate_airvisual_json():
    """Task: Validate AirVisual JSON"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_airvisual_to_postgresql', retries=2)
def load_airvisual_to_postgresql():
    """Task: Load AirVisual Data to PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="extract_airvisual_data_pipeline", task_runner=SequentialTaskRunner())
def extract_airvisual_data_pipeline():
    """Main flow for extracting, validating, and loading AirVisual data."""
    extract_airvisual_data()
    validate_airvisual_json()
    load_airvisual_to_postgresql()


if __name__ == "__main__":
    extract_airvisual_data_pipeline()