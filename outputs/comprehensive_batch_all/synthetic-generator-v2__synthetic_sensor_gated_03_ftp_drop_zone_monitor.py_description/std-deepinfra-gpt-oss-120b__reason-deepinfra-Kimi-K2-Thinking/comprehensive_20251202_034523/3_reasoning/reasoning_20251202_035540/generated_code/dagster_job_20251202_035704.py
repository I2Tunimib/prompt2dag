from dagster import (
    op,
    job,
    ScheduleDefinition,
    RetryPolicy,
    OpExecutionContext,
    Config,
    ResourceParam,
    Definitions,
    DefaultScheduleStatus,
)
import time
import os
import tempfile
from typing import Optional
from datetime import datetime, timedelta

# Note: pandas is required for data cleansing
# Install with: pip install pandas
try:
    import pandas as pd
except ImportError:
    pd = None


class FTPResource:
    """Stub for FTP connection resource.
    
    In production, this would contain actual FTP connection logic
    using libraries like ftplib or paramiko for SFTP.
    """
    
    def __init__(self, host: str, username: str, password: str, port: int = 21):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
    
    def file_exists(self, filename: str) -> bool:
        # Stub: In real implementation, connect to FTP and check file
        # For testing, we'll simulate file existence after some time
        return False
    
    def download_file(self, remote_path: str, local_path: str):
        # Stub: In real implementation, download from FTP
        pass


class InternalInventoryResource:
    """Stub for internal inventory system resource.
    
    In production, this would connect to your internal database
    or API (e.g., PostgreSQL, MySQL, REST API).
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def merge_inventory(self, vendor_data_path: str):
        # Stub: In real implementation, perform database merge
        pass


class FTPConfig(Config):
    """Configuration for FTP file waiting operation."""
    filename: str = "vendor_inventory.csv"
    poke_interval: int = 30  # seconds
    timeout: int = 300  # 5 minutes in seconds


class DownloadConfig(Config):
    """Configuration for file download operation."""
    local_dir: Optional[str] = None


@op(
    description="Polls FTP server for vendor inventory file presence."
)
def wait_for_ftp_file(
    context: OpExecutionContext,
    ftp_resource: ResourceParam[FTPResource],
    config: FTPConfig,
) -> bool:
    """Waits for vendor_inventory.csv on FTP server with 30s polling and 5min timeout."""
    start_time = datetime.now()
    timeout_delta = timedelta(seconds=config.timeout)
    
    context.log.info(
        f"Waiting for file '{config.filename}' on {ftp_resource.host} "
        f"(poke_interval={config.poke_interval}s, timeout={config.timeout}s)"
    )
    
    while datetime.now() - start_time < timeout_delta:
        if ftp_resource.file_exists(config.filename):
            context.log.info(f"File '{config.filename}' detected on FTP server")
            return True
        time.sleep(config.poke_interval)
    
    raise TimeoutError(
        f"File '{config.filename}' not found on FTP server after {config.timeout} seconds"
    )


@op(
    description="Downloads vendor inventory file from FTP to local temporary directory."
)
def download_vendor_file(
    context: OpExecutionContext,
    ftp_resource: ResourceParam[FTPResource],
    wait_result: bool,
    config: DownloadConfig,
) -> str:
    """Downloads file to local temp directory. Returns local file path."""
    if not wait_result:
        raise ValueError("Previous step indicated file was not found")
    
    if config.local_dir is None:
        config.local_dir = tempfile.gettempdir()
    
    local_path = os.path.join(config.local_dir, "vendor_inventory.csv")
    context.log.info(f"Downloading file to {local_path}")
    
    # Stub implementation: create sample file
    # In production: ftp_resource.download_file(config.filename, local_path)
    os.makedirs(config.local_dir, exist_ok=True)
    with open(local_path, 'w') as f:
        f.write("product_id,quantity,price\n")
        f.write("P001,10,9.99\n")
        f.write("P002,,19.99\n")  # Null quantity for testing
        f.write("P003,5,\n")      # Null price for testing
        f.write(",7,29.99\n")      # Null product_id for testing
    
    return local_path


@op(
    description="Cleanses vendor data by removing null values from key columns."
)
def cleanse_vendor_data(context: OpExecutionContext, file_path: str) -> str:
    """Removes nulls from product_id, quantity, price columns. Returns cleansed file path."""
    if pd is None:
        raise ImportError("pandas is required for data cleansing. Install with: pip install pandas")
    
    context.log.info(f"Reading and cleansing data from {file_path}")
    
    df = pd.read_csv(file_path)
    initial_count = len(df)
    
    # Remove rows with null values in specified columns
    df_clean = df.dropna(subset=['product_id', 'quantity', 'price'])
    final_count = len(df_clean)
    
    removed = initial_count - final_count
    context.log.info(f"Cleansed data: removed {removed} rows, kept {final_count} rows")
    
    # Write cleansed data to new file
    cleansed_path = file_path.replace('.csv', '_cleansed.csv')
    df_clean.to_csv(cleansed_path, index=False)
    
    return cleansed_path


@op(
    description="Merges cleansed vendor data with internal inventory system."
)
def merge_with_internal_inventory(
    context: OpExecutionContext,
    inventory_resource: ResourceParam[InternalInventoryResource],
    cleansed_file_path: str,
):
    """Performs join on product_id and updates stock levels and pricing."""
    context.log.info(
        f"Merging {cleansed_file_path} with internal inventory system "
        f"via {inventory_resource.connection_string}"
    )
    
    # Stub implementation
    # In production: inventory_resource.merge_inventory(cleansed_file_path)
    context.log.info("Merge operation completed successfully")


@job(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,  # 5 minutes between retries (in seconds)
    ),
    description="Daily pipeline to process vendor inventory from FTP server."
)
def vendor_inventory_pipeline():
    """Linear pipeline: wait -> download -> cleanse -> merge."""
    wait_result = wait_for_ftp_file()
    download_result = download_vendor_file(wait_result)
    cleanse_result = cleanse_vendor_data(download_result)
    merge_with_internal_inventory(cleanse_result)


# Daily schedule at midnight
vendor_inventory_schedule = ScheduleDefinition(
    job=vendor_inventory_pipeline,
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs vendor inventory pipeline daily at midnight",
)


# Definitions object to tie together jobs, schedules, and resources
defs = Definitions(
    jobs=[vendor_inventory_pipeline],
    schedules=[vendor_inventory_schedule],
    resources={
        "ftp_resource": FTPResource(
            host="ftp.example.com",
            username="ftp_user",
            password="ftp_password",
            port=21,
        ),
        "inventory_resource": InternalInventoryResource(
            connection_string="postgresql://user:pass@localhost:5432/inventory",
        ),
    },
)


if __name__ == "__main__":
    # Execute the job directly for testing
    # In production, run with: dagster dev
    result = vendor_inventory_pipeline.execute_in_process()
    assert result.success