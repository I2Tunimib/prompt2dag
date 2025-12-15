from prefect import flow, task, SequentialTaskRunner

@task(name='check_and_download_ensembl_files', retries=3)
def check_and_download_ensembl_files():
    """Task: Check and Download Ensembl Files"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='process_ensembl_files_with_spark', retries=3)
def process_ensembl_files_with_spark():
    """Task: Process Ensembl Files with Spark"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="etl_import_ensembl", task_runner=SequentialTaskRunner)
def etl_import_ensembl_flow():
    check_and_download_ensembl_files()
    process_ensembl_files_with_spark()

if __name__ == "__main__":
    etl_import_ensembl_flow()