from datetime import timedelta
import os
import tempfile
import time

import pandas as pd
from dagster import ConfigurableResource, RetryPolicy, job, op, resource


class FTPResource(ConfigurableResource):
    """Simple FTP resource stub."""

    host: str
    username: str
    password: str

    def file_exists(self, filename: str) -> bool:
        """Placeholder implementation that pretends the file appears after a short delay."""
        # In a real implementation, connect to the FTP server and check for the file.
        return True

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Placeholder implementation that writes a dummy CSV file."""
        df = pd.DataFrame(
            {
                "product_id": [1, 2, 3],
                "quantity": [10, 20, None],
                "price": [100.0, None, 300.0],
            }
        )
        df.to_csv(local_path, index=False)


class InternalInventoryResource(ConfigurableResource):
    """Resource that provides internal inventory data."""

    path: str = "internal_inventory.csv"

    def load_inventory(self) -> pd.DataFrame:
        """Load internal inventory, creating a dummy file if missing."""
        if not os.path.exists(self.path):
            df = pd.DataFrame(
                {
                    "product_id": [1, 2, 3, 4],
                    "quantity": [5, 5, 5, 5],
                    "price": [90.0, 190.0, 290.0, 390.0],
                }
            )
            df.to_csv(self.path, index=False)
        return pd.read_csv(self.path)


@resource
def ftp_resource(init_context) -> FTPResource:
    """Instantiate FTPResource with placeholder credentials."""
    return FTPResource(host="ftp.example.com", username="user", password="pass")


@resource
def internal_inventory_resource(init_context) -> InternalInventoryResource:
    """Instantiate InternalInventoryResource."""
    return InternalInventoryResource()


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"ftp"},
)
def wait_for_ftp_file(context) -> str:
    """Poll the FTP server until the target file appears or timeout occurs."""
    filename = "vendor_inventory.csv"
    timeout_seconds = 5 * 60
    interval = 30
    elapsed = 0

    while elapsed < timeout_seconds:
        if context.resources.ftp.file_exists(filename):
            context.log.info(f"File {filename} detected on FTP server.")
            return filename
        context.log.info(f"File {filename} not found; waiting {interval}s.")
        time.sleep(interval)
        elapsed += interval

    raise Exception(f"Timeout: {filename} not found after {timeout_seconds}s.")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"ftp"},
)
def download_vendor_file(context, filename: str) -> str:
    """Download the vendor file from FTP to a temporary local directory."""
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, filename)
    context.resources.ftp.download_file(filename, local_path)
    context.log.info(f"Downloaded {filename} to {local_path}.")
    return local_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def cleanse_vendor_data(context, file_path: str) -> pd.DataFrame:
    """Remove rows with null values in critical columns."""
    df = pd.read_csv(file_path)
    before = len(df)
    df = df.dropna(subset=["product_id", "quantity", "price"])
    after = len(df)
    context.log.info(f"Cleansed data: dropped {before - after} rows with nulls.")
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"internal_inventory"},
)
def merge_with_internal_inventory(context, vendor_df: pd.DataFrame) -> pd.DataFrame:
    """Merge vendor data with internal inventory and update stock/price."""
    internal_df = context.resources.internal_inventory.load_inventory()
    merged = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        suffixes=("_internal", "_vendor"),
        how="inner",
    )
    merged["quantity"] = merged["quantity_vendor"]
    merged["price"] = merged["price_vendor"]
    result = merged[["product_id", "quantity", "price"]]
    context.log.info(f"Merged inventory contains {len(result)} rows.")
    return result


@job(
    resource_defs={
        "ftp": ftp_resource,
        "internal_inventory": internal_inventory_resource,
    }
)
def vendor_inventory_job():
    """Linear pipeline: wait → download → cleanse → merge."""
    filename = wait_for_ftp_file()
    local_path = download_vendor_file(filename)
    cleaned = cleanse_vendor_data(local_path)
    merge_with_internal_inventory(cleaned)


if __name__ == "__main__":
    execution_result = vendor_inventory_job.execute_in_process()
    assert execution_result.success, "Job execution failed."