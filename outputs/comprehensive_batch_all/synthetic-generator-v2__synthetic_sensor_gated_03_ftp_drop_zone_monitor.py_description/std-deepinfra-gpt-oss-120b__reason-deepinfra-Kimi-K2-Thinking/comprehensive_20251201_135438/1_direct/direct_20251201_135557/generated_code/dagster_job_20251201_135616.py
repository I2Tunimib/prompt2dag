from __future__ import annotations

import csv
import os
import tempfile
import time
from datetime import timedelta
from typing import List

import pandas as pd
from dagster import (
    ConfigurableResource,
    Definitions,
    RetryPolicy,
    JobDefinition,
    OpExecutionContext,
    ResourceDefinition,
    ScheduleDefinition,
    job,
    op,
    schedule,
)


class FTPResource(ConfigurableResource):
    """Simple FTP resource using ftplib.

    In a real deployment, replace the placeholder connection logic with proper
    error handling and secure credential management.
    """

    host: str
    username: str
    password: str
    port: int = 21

    def get_ftp(self):
        import ftplib

        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port, timeout=30)
        ftp.login(self.username, self.password)
        return ftp


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Polls the FTP server for the presence of vendor_inventory.csv.",
    required_resource_keys={"ftp"},
)
def wait_for_ftp_file(context: OpExecutionContext) -> str:
    """Polls the FTP server every 30 seconds for up to 5 minutes."""
    ftp: FTPResource = context.resources.ftp
    filename = "vendor_inventory.csv"
    poke_interval = 30  # seconds
    timeout = 5 * 60  # 5 minutes

    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise RuntimeError(
                f"Timeout of {timeout} seconds reached while waiting for {filename} on FTP."
            )
        try:
            with ftp.get_ftp() as connection:
                files: List[str] = connection.nlst()
                if filename in files:
                    context.log.info(f"Detected file {filename} on FTP.")
                    return filename
                else:
                    context.log.info(f"{filename} not yet present. Waiting...")
        except Exception as exc:
            context.log.error(f"Error checking FTP: {exc}")
            # Continue polling; errors are not considered fatal here.

        time.sleep(poke_interval)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Downloads the detected vendor file to a local temporary directory.",
    required_resource_keys={"ftp"},
)
def download_vendor_file(context: OpExecutionContext, filename: str) -> str:
    """Downloads the file from FTP and returns the local path."""
    ftp: FTPResource = context.resources.ftp
    local_dir = tempfile.mkdtemp(prefix="vendor_")
    local_path = os.path.join(local_dir, filename)

    context.log.info(f"Downloading {filename} to {local_path}")
    try:
        with ftp.get_ftp() as connection:
            with open(local_path, "wb") as f:
                connection.retrbinary(f"RETR {filename}", f.write)
    except Exception as exc:
        raise RuntimeError(f"Failed to download {filename} from FTP: {exc}")

    return local_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Cleanses vendor data by dropping rows with null values in key columns.",
)
def cleanse_vendor_data(context: OpExecutionContext, file_path: str) -> str:
    """Reads CSV, drops rows with nulls in product_id, quantity, price, writes cleaned CSV."""
    context.log.info(f"Reading vendor data from {file_path}")
    df = pd.read_csv(file_path)

    required_columns = ["product_id", "quantity", "price"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in vendor data: {missing}")

    before = len(df)
    df_clean = df.dropna(subset=required_columns)
    after = len(df_clean)
    context.log.info(f"Dropped {before - after} rows with null values.")

    cleaned_dir = tempfile.mkdtemp(prefix="cleaned_")
    cleaned_path = os.path.join(cleaned_dir, "vendor_inventory_cleaned.csv")
    df_clean.to_csv(cleaned_path, index=False)
    context.log.info(f"Cleaned data written to {cleaned_path}")

    return cleaned_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Merges cleansed vendor data with internal inventory.",
)
def merge_with_internal_inventory(
    context: OpExecutionContext, cleaned_file_path: str
) -> str:
    """Performs a join on product_id and updates stock levels and pricing."""
    context.log.info(f"Reading cleaned vendor data from {cleaned_file_path}")
    vendor_df = pd.read_csv(cleaned_file_path)

    # Placeholder internal inventory path; replace with actual source in production.
    internal_inventory_path = os.path.join(
        os.path.dirname(__file__), "internal_inventory.csv"
    )
    if not os.path.exists(internal_inventory_path):
        # Create a dummy internal inventory for demonstration purposes.
        dummy_data = {
            "product_id": vendor_df["product_id"].unique(),
            "stock_level": [0] * len(vendor_df["product_id"].unique()),
            "price": [0.0] * len(vendor_df["product_id"].unique()),
        }
        pd.DataFrame(dummy_data).to_csv(internal_inventory_path, index=False)

    context.log.info(f"Reading internal inventory from {internal_inventory_path}")
    internal_df = pd.read_csv(internal_inventory_path)

    merged_df = pd.merge(
        internal_df,
        vendor_df[["product_id", "quantity", "price"]],
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )

    # Update stock level and price where vendor data is present.
    merged_df["stock_level"] = merged_df["stock_level"] + merged_df["quantity"].fillna(0)
    merged_df["price"] = merged_df["price_vendor"].combine_first(merged_df["price_internal"])

    # Drop auxiliary columns.
    merged_df = merged_df.drop(columns=["quantity", "price_internal", "price_vendor"])

    output_dir = tempfile.mkdtemp(prefix="merged_")
    output_path = os.path.join(output_dir, "merged_inventory.csv")
    merged_df.to_csv(output_path, index=False)
    context.log.info(f"Merged inventory written to {output_path}")

    return output_path


@job(
    description="Sensor‑gated pipeline that processes vendor inventory from FTP.",
    resource_defs={"ftp": FTPResource()},
)
def vendor_inventory_job():
    filename = wait_for_ftp_file()
    local_path = download_vendor_file(filename)
    cleaned_path = cleanse_vendor_data(local_path)
    merge_with_internal_inventory(cleaned_path)


daily_schedule = ScheduleDefinition(
    job=vendor_inventory_job,
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    description="Runs the vendor inventory job once per day.",
)


defs = Definitions(
    jobs=[vendor_inventory_job],
    schedules=[daily_schedule],
    resources={"ftp": FTPResource(host="ftp.example.com", username="user", password="pass")},
)


if __name__ == "__main__":
    result = vendor_inventory_job.execute_in_process()
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")