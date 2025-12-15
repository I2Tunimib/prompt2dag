from prefect import flow, task, get_run_logger
from prefect.orion.schemas.schedules import CronSchedule
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
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

@flow(name="check_pcd_sftp_folder_pipeline", task_runner=ConcurrentTaskRunner())
def check_pcd_sftp_folder_pipeline():
    logger = get_run_logger()
    logger.info("Starting the PCD SFTP Folder Pipeline")

    check_sftp = check_pcd_sftp_folder()
    check_shared = check_pcd_shared_folder(wait_for=[check_sftp])
    start_extract_1 = start_pcd_extract_1(wait_for=[check_shared])
    parallel_api = parallel_http_api_extraction(wait_for=[start_extract_1])
    start_extract_2 = start_pcd_extract_2(wait_for=[parallel_api])
    upload_file = pcd_file_upload(wait_for=[start_extract_2])
    notify = etl_notification(wait_for=[upload_file])

    logger.info("PCD SFTP Folder Pipeline completed successfully")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=check_pcd_sftp_folder_pipeline,
        name="check_pcd_sftp_folder_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="{{var.value.pcd_etl_schedule}}", timezone="UTC"),
        parameters={},
        infra_overrides={"image": "python:3.9"},
        work_queue_name="default",
    )
    deployment.apply()