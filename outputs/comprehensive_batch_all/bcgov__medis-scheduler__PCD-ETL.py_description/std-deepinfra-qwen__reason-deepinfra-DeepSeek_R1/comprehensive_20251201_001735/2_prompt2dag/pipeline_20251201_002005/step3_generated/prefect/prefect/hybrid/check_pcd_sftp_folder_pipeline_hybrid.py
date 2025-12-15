from prefect import flow, task, get_run_logger
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
    logger.info("Starting the check_pcd_sftp_folder_pipeline")

    check_sftp_folder_result = check_pcd_sftp_folder.submit()
    check_shared_folder_result = check_pcd_shared_folder.submit(wait_for=[check_sftp_folder_result])
    start_extract_1_result = start_pcd_extract_1.submit(wait_for=[check_shared_folder_result])
    parallel_http_api_extraction_result = parallel_http_api_extraction.submit(wait_for=[start_extract_1_result])
    start_extract_2_result = start_pcd_extract_2.submit(wait_for=[parallel_http_api_extraction_result])
    pcd_file_upload_result = pcd_file_upload.submit(wait_for=[start_extract_2_result])
    etl_notification.submit(wait_for=[pcd_file_upload_result])

    logger.info("check_pcd_sftp_folder_pipeline completed successfully")

if __name__ == "__main__":
    check_pcd_sftp_folder_pipeline()