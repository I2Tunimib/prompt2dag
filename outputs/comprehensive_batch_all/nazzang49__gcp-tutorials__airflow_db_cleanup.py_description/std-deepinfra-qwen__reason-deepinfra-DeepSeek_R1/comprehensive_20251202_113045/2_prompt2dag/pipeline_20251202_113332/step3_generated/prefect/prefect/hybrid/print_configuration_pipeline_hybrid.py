from prefect import flow, task, get_run_logger
from prefect.orion.schemas.states import Completed
from prefect.task_runners import SequentialTaskRunner

@task(name='print_configuration', retries=1)
def print_configuration():
    """Task: Print Configuration"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanup_airflow_metadb', retries=1)
def cleanup_airflow_metadb():
    """Task: Cleanup Airflow MetaDB"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="print_configuration_pipeline", task_runner=SequentialTaskRunner)
def print_configuration_pipeline():
    logger = get_run_logger()
    logger.info("Starting print_configuration_pipeline")

    # Task: print_configuration
    print_configuration_result = print_configuration.submit()

    # Task: cleanup_airflow_metadb
    cleanup_airflow_metadb.submit(wait_for=[print_configuration_result])

if __name__ == "__main__":
    print_configuration_pipeline()