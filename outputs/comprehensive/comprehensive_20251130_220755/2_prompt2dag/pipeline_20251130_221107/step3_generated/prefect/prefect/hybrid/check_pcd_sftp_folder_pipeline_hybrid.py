from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Completed
from prefect.task_runners import ConcurrentTaskRunner

@task(name='check_pcd_sftp_folder', retries=0)
def check_pcd_sftp_folder():
    """Task: Check PCD SFTP Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='check_pcd_shared_folder', retries=0)
def check_pcd_shared_folder():
    """Task: Check PCD Shared Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='start_pcd_extract_1', retries=0)
def start_pcd_extract_1():
    """Task: Start PCD Extract 1"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='parallel_http_api_extraction', retries=0)
def parallel_http_api_extraction():
    """Task: Parallel HTTP API Extraction"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='start_pcd_extract_2', retries=0)
def start_pcd_extract_2():
    """Task: Start PCD Extract 2"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='pcd_file_upload', retries=0)
def pcd_file_upload():
    """Task: PCD File Upload"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='etl_notification', retries=0)
def etl_notification():
    """Task: ETL Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="check_pcd_sftp_folder_pipeline", task_runner=ConcurrentTaskRunner)
def check_pcd_sftp_folder_pipeline():
    logger = get_run_logger()
    logger.info("Starting check_pcd_sftp_folder_pipeline")

    # Entry point
    sftp_folder_check = check_pcd_sftp_folder.submit()

    # Dependencies
    shared_folder_check = check_pcd_shared_folder.submit(wait_for=[sftp_folder_check])
    extract_1_start = start_pcd_extract_1.submit(wait_for=[shared_folder_check])
    api_extraction = parallel_http_api_extraction.submit(wait_for=[extract_1_start])
    extract_2_start = start_pcd_extract_2.submit(wait_for=[api_extraction])
    file_upload = pcd_file_upload.submit(wait_for=[extract_2_start])
    notification = etl_notification.submit(wait_for=[file_upload, api_extraction])

    logger.info("check_pcd_sftp_folder_pipeline completed successfully")

if __name__ == "__main__":
    check_pcd_sftp_folder_pipeline()