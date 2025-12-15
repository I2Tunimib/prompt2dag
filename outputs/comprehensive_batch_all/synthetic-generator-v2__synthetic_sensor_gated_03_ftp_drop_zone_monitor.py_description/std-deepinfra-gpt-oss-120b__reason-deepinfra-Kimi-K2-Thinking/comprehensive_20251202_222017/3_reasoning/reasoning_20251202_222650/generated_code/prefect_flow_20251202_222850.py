import os
import time
import tempfile
from typing import Optional

import pandas as pd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


class MockFTPClient:
    """Mock FTP client for demonstration. Replace with ftplib.FTP in production."""
    
    def __init__(self, host: str, username: str, password: str):
        self.host = host
        self.username = username
        self.password = password
        self.connected = False
        self._file_exists = False
        self._check_count = 0
    
    def connect(self):
        """Simulate FTP connection."""
        self.connected = True
        print(f"✓ Connected to FTP server: {self.host}")
    
    def file_exists(self, filename: str) -> bool:
        """
        Simulate checking for file existence.
        In production, use: filename in ftp.nlst()
        """
        self._check_count += 1
        
        # Simulate file appearing after 2 checks
        if self._check_count >= 2:
            self._file_exists = True
        
        exists = self._file_exists
        status = "✓ FOUND" if exists else "✗ NOT FOUND"
        print(f"  Check #{self._check_count}: {status} - '{filename}'")
        return exists
    
    def download_file(self, remote_path: str, local_path: str):
        """Simulate file download from FTP."""
        sample_data = """product_id,quantity,price
P001,10,9.99
P002,5,19.99
P003,8,15.99
P004,12,7.99
"""
        with open(local_path, 'w') as f:
            f.write(sample_data)
        print(f"✓ Downloaded '{remote_path}' to '{local_path}'")
    
    def close(self):
        """Close FTP connection."""
        self.connected = False
        print("✓ FTP connection closed")


@task(
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
    description="Poll FTP server every 30s for vendor_inventory.csv"
)
def wait_for_ftp_file(
    ftp_host: str,
    ftp_username: str,
    ftp_password: str,
    filename: str = "vendor_inventory.csv",
    poke_interval: int = 30
) -> bool:
    """
    Sensor-like task that polls FTP server for file presence.
    Waits until file is found or 5-minute timeout is reached.
    """
    ftp_client = MockFTPClient(ftp_host, ftp_username, ftp_password)
    ftp_client.connect()
    
    try:
        while True:
            if ftp_client.file_exists(filename):
                print(f"→ File '{filename}' detected on FTP server")
                return True
            
            print(f"→ Waiting {poke_interval}s before next check...")
            time.sleep(poke_interval)
    finally:
        ftp_client.close()


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Download vendor file from FTP to local temp directory"
)
def download_vendor_file(
    ftp_host: str,
    ftp_username: str,
    ftp_password: str,
    filename: str = "vendor_inventory.csv",
    local_dir: Optional[str] = None
) -> str:
    """
    Download vendor inventory file from FTP server.
    Returns path to local file.
    """
    if local_dir is None:
        local_dir = tempfile.gettempdir()
    
    local_path = os.path.join(local_dir, filename)
    
    ftp_client = MockFTPClient(ftp_host, ftp_username, ftp_password)
    ftp_client.connect()
    ftp_client.download_file(filename, local_path)
    ftp_client.close()
    
    print(f"→ File saved to: {local_path}")
    return local_path


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Cleanse vendor data by removing nulls from critical columns"
)
def cleanse_vendor_data(file_path: str) -> str:
    """
    Cleanse vendor data: remove rows with null values in product_id, quantity, price.
    Returns path to cleansed CSV file.
    """
    print(f"→ Reading vendor data from: {file_path}")
    df = pd.read_csv(file_path)
    
    initial_count = len(df)
    columns_to_check = ['product_id', 'quantity', 'price']
    df_clean = df.dropna(subset=columns_to_check)
    
    removed_count = initial_count - len(df_clean)
    print(f"→ Cleaned data: removed {removed_count} rows with null values")
    
    cleansed_path = file_path.replace('.csv', '_cleansed.csv')
    df_clean.to_csv(cleansed_path, index=False)
    print(f"→ Cleansed data saved to: {cleansed_path}")
    
    return cleansed_path


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Merge