import os
import time
import tempfile
from typing import Any

import pandas as pd
from ftplib import FTP, error_perm

from dagster import (
    InitResourceContext,
    RetryPolicy,
    ResourceDefinition,
    op,
    job,
)


class FTPResource:
    """Simple FTP resource wrapper."""

    def __init__(self, host: str, user: str, password: str) -> None:
        self.host = host
        self.user = user
        self.password = password

    def get_client(self) -> FTP:
        """Create and return an active FTP client."""
        client = FTP(self.host, timeout=30)
        client.login(self.user, self.password)
        return client


def ftp_resource_factory(context: InitResourceContext) -> FTPResource:
    """Factory for the FTP resource.

    In a real deployment, credentials would come from secrets management.
    """
    return FTPResource(
        host="ftp.example.com",
        user="ftp_user",
        password="ftp_password",
    )


ftp_resource = ResourceDefinition.hardcoded_resource(ftp_resource_factory(None))


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Polls the FTP server until vendor_inventory.csv appears.",
)
def wait_for_ftp_file(context) -> str:
    """Wait for the vendor file to appear on the FTP server.

    Returns the remote filename when found.
    """
    file_name = "vendor_inventory.csv"
    poke_interval = 30  # seconds
    timeout = 5 * 60  # 5 minutes

    ftp: FTP = context.resources.ftp.get_client()
    start = time.time()
    while time.time() - start < timeout:
        try:
            files = ftp.nlst()
            if file_name in files:
                context.log.info(f"Detected file '{file_name}' on FTP server.")
                return file_name
        except error_perm as exc:
            context.log.error(f"FTP permission error while listing files: {exc}")
        context.log.info(f"File '{file_name}' not found. Sleeping {poke_interval}s.")
        time.sleep(poke_interval)

    raise RuntimeError(f"Timeout: '{file_name}' not found on FTP server within {timeout}s.")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Downloads the vendor file from the FTP server to a local temporary directory.",
)
def download_vendor_file(context, remote_file: str) -> str:
    """Download the remote CSV file to a temporary local path."""
    ftp: FTP = context.resources.ftp.get_client()
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, remote_file)

    with open(local_path, "wb") as f:
        def _callback(data: bytes) -> None:
            f.write(data)

        context.log.info(f"Starting download of '{remote_file}' to '{local_path}'.")
        ftp.retrbinary(f"RETR {remote_file}", _callback)

    context.log.info(f"Download completed: {local_path}")
    return local_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Cleanses the vendor CSV by dropping rows with nulls in key columns.",
)
def cleanse_vendor_data(context, vendor_path: str) -> str:
    """Read, cleanse, and write the vendor data."""
    df = pd.read_csv(vendor_path)
    required_cols = ["product_id", "quantity", "price"]
    before = len(df)
    df_clean = df.dropna(subset=required_cols)
    after = len(df_clean)
    context.log.info(f"Dropped {before - after} rows with nulls in {required_cols}.")

    clean_dir = tempfile.mkdtemp()
    clean_path = os.path.join(clean_dir, "vendor_inventory_clean.csv")
    df_clean.to_csv(clean_path, index=False)
    context.log.info(f"Cleansed data written to {clean_path}.")
    return clean_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Merges cleansed vendor data with internal inventory.",
)
def merge_with_internal_inventory(context, clean_vendor_path: str) -> str:
    """Merge vendor data with internal inventory and output the result."""
    vendor_df = pd.read_csv(clean_vendor_path)

    # In a real system, internal inventory would be fetched from a database or service.
    # Here we create a minimal placeholder DataFrame.
    internal_data = {
        "product_id": [1, 2, 3],
        "stock_level": [100, 150, 200],
        "price": [10.0, 20.0, 30.0],
    }
    internal_df = pd.DataFrame(internal_data)

    merged_df = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )

    # Update stock levels and price where vendor data is present.
    merged_df["stock_level"] = merged_df.apply(
        lambda row: row["quantity"] if pd.notnull(row["quantity"]) else row["stock_level"],
        axis=1,
    )
    merged_df["price"] = merged_df.apply(
        lambda row: row["price_vendor"] if pd.notnull(row["price_vendor"]) else row["price_internal"],
        axis=1,
    )
    merged_df = merged_df[["product_id", "stock_level", "price"]]

    output_dir = tempfile.mkdtemp()
    output_path = os.path.join(output_dir, "merged_inventory.csv")
    merged_df.to_csv(output_path, index=False)
    context.log.info(f"Merged inventory written to {output_path}.")
    return output_path


@job(resource_defs={"ftp": ftp_resource})
def vendor_inventory_job():
    """Dagster job orchestrating the vendor inventory pipeline."""
    file_name = wait_for_ftp_file()
    local_path = download_vendor_file(file_name)
    clean_path = cleanse_vendor_data(local_path)
    merge_with_internal_inventory(clean_path)


if __name__ == "__main__":
    result = vendor_inventory_job.execute_in_process()
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")