from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='wait_for_ftp_file', retries=2)
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


@flow(name="wait_for_ftp_file_pipeline", task_runner=SequentialTaskRunner())
def wait_for_ftp_file_pipeline():
    """Main flow orchestrating the FTP file processing pipeline."""
    wait_for_ftp_file()
    download_vendor_file()
    cleanse_vendor_data()
    merge_with_internal_inventory()


if __name__ == "__main__":
    wait_for_ftp_file_pipeline()