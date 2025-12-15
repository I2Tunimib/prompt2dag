from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner

@task(name='wait_for_ftp_file', retries=0)
def wait_for_ftp_file():
    """Task: Wait for FTP File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_vendor_file', retries=2)
def download_vendor_file():
    """Task: Download Vendor File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanse_vendor_data', retries=2)
def cleanse_vendor_data():
    """Task: Cleanse Vendor Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='merge_with_internal_inventory', retries=2)
def merge_with_internal_inventory():
    """Task: Merge with Internal Inventory"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="wait_for_ftp_file_pipeline", task_runner=SequentialTaskRunner)
def wait_for_ftp_file_pipeline():
    logger = get_run_logger()
    logger.info("Starting wait_for_ftp_file_pipeline")

    wait_for_ftp_file_result = wait_for_ftp_file()
    download_vendor_file_result = download_vendor_file(wait_for_ftp_file_result)
    cleanse_vendor_data_result = cleanse_vendor_data(download_vendor_file_result)
    merge_with_internal_inventory_result = merge_with_internal_inventory(cleanse_vendor_data_result)

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=wait_for_ftp_file_pipeline,
        name="wait_for_ftp_file_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *"),  # @daily
    )
    deployment.apply()