from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import time
import os
import ftplib
import pandas as pd

# Constants
FTP_SERVER = "ftp.example.com"
FTP_USERNAME = "user"
FTP_PASSWORD = "password"
FTP_FILE_NAME = "vendor_inventory.csv"
LOCAL_TEMP_DIR = "/tmp/vendor_inventory"
INTERNAL_INVENTORY_PATH = "/path/to/internal_inventory.csv"
RETRY_DELAY = timedelta(minutes=5)
RETRIES = 2

# Task Definitions
@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def wait_for_ftp_file():
    logger = get_run_logger()
    start_time = time.time()
    timeout = 300  # 5 minutes
    interval = 30  # 30 seconds
    while True:
        try:
            with ftplib.FTP(FTP_SERVER) as ftp:
                ftp.login(FTP_USERNAME, FTP_PASSWORD)
                if FTP_FILE_NAME in ftp.nlst():
                    logger.info(f"File {FTP_FILE_NAME} detected on FTP server.")
                    return
        except Exception as e:
            logger.error(f"Error checking FTP server: {e}")
        if time.time() - start_time > timeout:
            logger.error(f"File {FTP_FILE_NAME} not found within {timeout} seconds.")
            raise TimeoutError(f"File {FTP_FILE_NAME} not found within {timeout} seconds.")
        time.sleep(interval)

@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def download_vendor_file():
    logger = get_run_logger()
    local_file_path = os.path.join(LOCAL_TEMP_DIR, FTP_FILE_NAME)
    with ftplib.FTP(FTP_SERVER) as ftp:
        ftp.login(FTP_USERNAME, FTP_PASSWORD)
        with open(local_file_path, 'wb') as file:
            ftp.retrbinary(f"RETR {FTP_FILE_NAME}", file.write)
    logger.info(f"File {FTP_FILE_NAME} downloaded to {local_file_path}.")
    return local_file_path

@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def cleanse_vendor_data(file_path):
    logger = get_run_logger()
    df = pd.read_csv(file_path)
    df.dropna(subset=["product_id", "quantity", "price"], inplace=True)
    logger.info("Vendor data cleansed.")
    return df

@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def merge_with_internal_inventory(cleansed_data):
    logger = get_run_logger()
    internal_inventory = pd.read_csv(INTERNAL_INVENTORY_PATH)
    merged_inventory = pd.merge(internal_inventory, cleansed_data, on="product_id", how="left")
    merged_inventory.to_csv(INTERNAL_INVENTORY_PATH, index=False)
    logger.info("Vendor data merged with internal inventory.")

# Flow Definition
@flow(name="Vendor Inventory Pipeline")
def vendor_inventory_pipeline():
    wait_for_ftp_file()
    file_path = download_vendor_file()
    cleansed_data = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleansed_data)

if __name__ == "__main__":
    vendor_inventory_pipeline()

# Optional: Deployment/Schedule Configuration
# To schedule this flow to run daily, you can use Prefect's deployment and schedule features.
# Example:
# from prefect.deployments import Deployment
# from prefect.orion.schemas.schedules import IntervalSchedule
# from datetime import timedelta

# deployment = Deployment.build_from_flow(
#     flow=vendor_inventory_pipeline,
#     name="Daily Vendor Inventory Pipeline",
#     schedule=IntervalSchedule(interval=timedelta(days=1)),
# )
# deployment.apply()