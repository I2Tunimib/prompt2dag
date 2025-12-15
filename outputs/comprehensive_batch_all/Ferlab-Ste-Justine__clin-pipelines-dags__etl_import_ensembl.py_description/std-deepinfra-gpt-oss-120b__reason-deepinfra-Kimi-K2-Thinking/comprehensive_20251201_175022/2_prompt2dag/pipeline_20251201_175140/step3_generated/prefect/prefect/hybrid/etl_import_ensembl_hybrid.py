from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='extract_ensembl_files', retries=0)
def extract_ensembl_files():
    """Task: Extract Ensembl Mapping Files"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_ensembl_mapping', retries=0)
def transform_ensembl_mapping():
    """Task: Transform Ensembl Mapping with Spark"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="etl_import_ensembl", task_runner=SequentialTaskRunner())
def etl_import_ensembl():
    """Sequential ETL pipeline for importing Ensembl data."""
    # Entry task
    extract_ensembl_files()
    # Dependent task
    transform_ensembl_mapping()


if __name__ == "__main__":
    etl_import_ensembl()