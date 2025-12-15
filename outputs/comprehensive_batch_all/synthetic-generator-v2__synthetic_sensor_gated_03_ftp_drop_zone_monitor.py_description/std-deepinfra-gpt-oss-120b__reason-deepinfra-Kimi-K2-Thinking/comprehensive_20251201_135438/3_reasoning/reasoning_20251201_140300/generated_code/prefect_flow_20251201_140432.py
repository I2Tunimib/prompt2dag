import os
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash


class MockFTPClient:
    """Simulated FTP client for demonstration purposes."""
    
    def __init__(self, host: str, username: str, password: str):
        self.host = host
        self.username = username
        self.password = password
        self.connected = False
        self.poll_count = 0
    
    def connect(self):
        self.connected = True
        print(f"Connected to FTP server: {self.host}")
    
    def file_exists(self, filename: str) -> bool:
        """Simulates file checking; returns True after 3 polling attempts."""
        self.poll_count += 1
        if self.poll_count >= 3:
            return True
        return False
    
    def download(self, remote_path: str, local_path: str):
        """Simulates downloading and creates a mock CSV file."""
        print(f"Downloading {remote_path} to {local_path}")
        mock_data = {
            'product_id': ['P001', 'P002', None, 'P004', 'P005'],
            'quantity': [10, 20, 15, None, 50],
            'price': [9.99, 19.99, None, 29.99, 39.99],
            'vendor_name': ['Vendor A', 'Vendor B', 'Vendor C', 'Vendor D', 'Vendor E']
        }
        df = pd.DataFrame(mock_data)
        df.to_csv(local_path, index=False)
        print(f"Mock file created at: {local_path}")


@task(
    name="wait_for_ftp_file",
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def wait_for_ftp_file(
    ftp_host: str,
    ftp_username: str,
    ftp_password: str,
    filename: str = "vendor_inventory.csv",
    poke_interval: int = 30
) -> bool:
    """Polls FTP server for file; returns True when found or raises TimeoutError."""
    ftp_client = MockFTPClient(ftp_host, ftp_username, ftp_password)
    ftp_client.connect()
    
    start_time = time.time()
    timeout_seconds = 300
    
    while True:
        if ftp_client.file_exists(filename):
            print(f"File '{filename}' found on FTP server.")
            return True
        
        elapsed = time.time() - start_time
        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"File '{filename}' not found on FTP server after {timeout_seconds} seconds."
            )
        
        print(f"File '{filename}' not found. Waiting {poke_interval} seconds...")
        time.sleep(poke_interval)


@task(
    name="download_vendor_file",
    retries=2,
    retry_delay_seconds=300
)
def download_vendor_file(
    ftp_host: str,
    ftp_username: str,
    ftp_password: str,
    remote_filename: str = "vendor_inventory.csv",
    local_dir: Optional[str] = None
) -> str:
    """Downloads vendor file from FTP to local directory; returns local file path."""
    if local_dir is None:
        local_dir = Path("/tmp/vendor_inventory")
        local_dir.mkdir(parents=True, exist_ok=True)
    else:
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
    
    local_path = local_dir / remote_filename
    
    ftp_client = MockFTPClient(ftp_host, ftp_username, ftp_password)
    ftp_client.connect()
    ftp_client.download(remote_filename, str(local_path))
    
    print(f"File downloaded successfully to: {local_path}")
    return str(local_path)


@task(
    name="cleanse_vendor_data",
    retries=2,
    retry_delay_seconds=300
)
def cleanse_vendor_data(file_path: str) -> str:
    """Removes null values from product_id, quantity, price columns; returns cleansed file path."""
    df = pd.read_csv(file_path)
    print(f"Original data shape: {df.shape}")
    
    required_columns = ['product_id', 'quantity', 'price']
    cleansed_df = df.dropna(subset=required_columns)
    print(f"Cleansed data shape: {cleansed_df.shape}")
    
    cleansed_path = Path(file_path).parent / "vendor_inventory_cleansed.csv"
    cleansed_df.to_csv(cleansed_path, index=False)
    print(f"Cleansed data saved to: {cleansed_path}")
    
    return str(cleansed_path)


@task(
    name="merge_with_internal_inventory",
    retries=2,
    retry_delay_seconds=300
)
def merge_with_internal_inventory(cleansed_file_path: str) -> str:
    """Merges cleansed vendor data with internal inventory on product_id; returns merged file path."""
    vendor_df = pd.read_csv(cleansed_file_path)
    print(f"Vendor data shape: {vendor_df.shape}")
    
    internal_data = {
        'product_id': ['P001', 'P002', 'P003', 'P004', 'P006'],
        'internal_stock': [5, 25, 0, 30, 100],
        'internal_price': [10.99, 18.99, 15.99, 28.99, 45.99]
    }
    internal_df = pd.DataFrame(internal_data)
    print(f"Internal inventory shape: {internal_df.shape}")
    
    merged_df = internal_df.merge(
        vendor_df,
        on='product_id',
        how='left',
        suffixes=('_internal', '_vendor')
    )
    
    merged_df['final_stock'] = merged_df['quantity'].fillna(merged_df['internal_stock'])
    merged_df['final_price'] = merged_df['price'].fillna(merged_df['internal_price'])
    
    result_df = merged_df[['product_id', 'final_stock', 'final_price', 'vendor_name']]
    result_df.columns = ['product_id', 'stock_level', 'price', 'vendor_name']
    
    merged_path = Path(cleansed_file_path).parent / "merged_inventory.csv"
    result_df.to_csv(merged_path, index=False)
    
    print(f"Merged inventory saved to: {merged_path}")
    print(f"Merged data preview:\n{result_df.head()}")
    
    return str(merged_path)


@flow(name="vendor-inventory-pipeline")
def vendor_inventory_pipeline(
    ftp_host: str = "ftp.example.com",
    ftp_username: str = "demo_user",
    ftp_password: str = "demo_pass",
    vendor_filename: str = "vendor_inventory.csv"
):
    """Sensor-gated pipeline that monitors FTP server for vendor inventory file."""
    wait_for_ftp_file(
        ftp_host=ftp_host,
        ftp_username=ftp_username,
        ftp_password=ftp_password,
        filename=vendor_filename
    )
    
    downloaded_path = download_vendor_file(
        ftp_host=ftp_host,
        ftp_username=ftp_username,
        ftp_password=ftp_password,
        remote_filename=vendor_filename
    )
    
    cleansed_path = cleanse_vendor_data(downloaded_path)
    merge_with_internal_inventory(cleansed_path)


if __name__ == '__main__':
    # For production deployment with daily schedule, use:
    # prefect deployment build vendor_inventory_pipeline:vendor_inventory_pipeline \
    #   --name "daily-vendor-inventory" --cron "0 2 * * *" --apply
    vendor_inventory_pipeline(
        ftp_host="ftp.example.com",
        ftp_username="demo_user",
        ftp_password="demo_pass"
    )