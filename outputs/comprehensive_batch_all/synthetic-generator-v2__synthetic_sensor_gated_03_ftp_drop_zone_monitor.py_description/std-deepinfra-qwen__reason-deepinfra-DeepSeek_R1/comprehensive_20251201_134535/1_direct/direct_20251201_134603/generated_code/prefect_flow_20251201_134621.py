from prefect import flow, task
import time
from datetime import timedelta
from typing import Optional

# Simulated functions for FTP interaction and data processing
def poll_ftp_for_file(file_name: str, poke_interval: int, timeout: int) -> bool:
    """Simulates polling an FTP server for a file."""
    # Simulate file detection after 1 minute
    time.sleep(60)
    return True

def download_vendor_file(file_name: str) -> str:
    """Simulates downloading a file from an FTP server."""
    return f"/tmp/{file_name}"

def cleanse_vendor_data(file_path: str) -> str:
    """Simulates cleansing the vendor data."""
    return f"{file_path}.cleaned"

def merge_with_internal_inventory(cleaned_file_path: str) -> None:
    """Simulates merging the cleansed vendor data with the internal inventory."""
    print(f"Merged {cleaned_file_path} with internal inventory.")

@task(retries=2, retry_delay_seconds=300)
def wait_for_ftp_file(file_name: str) -> bool:
    """Waits for the vendor inventory file to be available on the FTP server."""
    return poll_ftp_for_file(file_name, poke_interval=30, timeout=300)

@task(retries=2, retry_delay_seconds=300)
def download_vendor_file_task(file_name: str) -> str:
    """Downloads the vendor inventory file from the FTP server."""
    return download_vendor_file(file_name)

@task(retries=2, retry_delay_seconds=300)
def cleanse_vendor_data_task(file_path: str) -> str:
    """Cleanses the vendor inventory data."""
    return cleanse_vendor_data(file_path)

@task(retries=2, retry_delay_seconds=300)
def merge_with_internal_inventory_task(cleaned_file_path: str) -> None:
    """Merges the cleansed vendor inventory data with the internal inventory."""
    merge_with_internal_inventory(cleaned_file_path)

@flow(name="Vendor Inventory Pipeline", retries=2, retry_delay_seconds=300)
def vendor_inventory_pipeline(file_name: str = "vendor_inventory.csv"):
    """Orchestrates the vendor inventory pipeline."""
    file_detected = wait_for_ftp_file(file_name)
    if file_detected:
        downloaded_file_path = download_vendor_file_task(file_name)
        cleansed_file_path = cleanse_vendor_data_task(downloaded_file_path)
        merge_with_internal_inventory_task(cleansed_file_path)

if __name__ == "__main__":
    vendor_inventory_pipeline()

# Optional: Add deployment/schedule configuration
# Example: Deploy this flow to run daily using Prefect's deployment and scheduling features.