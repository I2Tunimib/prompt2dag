from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

@task(name='load_cleanup_configuration', retries=1)
def load_cleanup_configuration():
    """Task: Load Cleanup Configuration"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanup_airflow_metadb', retries=1)
def cleanup_airflow_metadb():
    """Task: Cleanup Airflow MetaDB"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="airflow_database_cleanup", task_runner=SequentialTaskRunner())
def airflow_database_cleanup():
    load_cleanup_configuration()
    cleanup_airflow_metadb()

if __name__ == "__main__":
    airflow_database_cleanup()