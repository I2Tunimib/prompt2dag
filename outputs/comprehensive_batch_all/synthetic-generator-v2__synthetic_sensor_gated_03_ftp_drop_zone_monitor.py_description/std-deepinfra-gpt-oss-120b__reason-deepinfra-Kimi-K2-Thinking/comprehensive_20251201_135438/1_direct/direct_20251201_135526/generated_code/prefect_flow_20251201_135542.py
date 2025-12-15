import os
import time
import tempfile
from pathlib import Path
from typing import Any

import ftplib
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


FTP_HOST = os.getenv("FTP_HOST", "ftp.example.com")
FTP_USER = os.getenv("FTP_USER", "anonymous")
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "")
FTP_DIRECTORY = os.getenv("FTP_DIRECTORY", "/")
VENDOR_FILENAME = "vendor_inventory.csv"
INTERNAL_INVENTORY_PATH = os.getenv(
    "INTERNAL_INVENTORY_PATH", "internal_inventory.csv"
)
MERGED_OUTPUT_PATH = os.getenv(
    "MERGED_OUTPUT_PATH", "merged_inventory.csv"
)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Poll FTP server until the vendor file appears.",
)
def wait_for_ftp_file() -> str:
    """
    Poll the FTP server every 30 seconds for up to 5 minutes
    to detect the presence of ``vendor_inventory.csv``.
    Returns the filename when found; raises an exception on timeout.
    """
    poke_interval = 30  # seconds
    timeout = 5 * 60  # seconds
    attempts = timeout // poke_interval

    for attempt in range(int(attempts)):
        try:
            with ftplib.FTP(FTP_HOST) as ftp:
                ftp.login(FTP_USER, FTP_PASSWORD)
                ftp.cwd(FTP_DIRECTORY)
                files = ftp.nlst()
                if VENDOR_FILENAME in files:
                    return VENDOR_FILENAME
        except ftplib.all_errors as exc:
            raise RuntimeError(f"FTP connection error: {exc}") from exc

        time.sleep(poke_interval)

    raise TimeoutError(
        f"File {VENDOR_FILENAME} not found on FTP after {timeout} seconds."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Download the vendor file from FTP to a temporary location.",
)
def download_vendor_file(filename: str) -> Path:
    """
    Retrieves ``filename`` from the FTP server and stores it in a temporary
    directory. Returns the path to the downloaded file.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / filename
        try:
            with ftplib.FTP(FTP_HOST) as ftp:
                ftp.login(FTP_USER, FTP_PASSWORD)
                ftp.cwd(FTP_DIRECTORY)
                with open(local_path, "wb") as f:
                    ftp.retrbinary(f"RETR {filename}", f.write)
        except ftplib.all_errors as exc:
            raise RuntimeError(f"Failed to download {filename}: {exc}") from exc

        # Move file to a persistent location before the temporary directory is cleaned up
        persistent_path = Path.cwd() / filename
        local_path.replace(persistent_path)
        return persistent_path


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Cleanse vendor data by dropping rows with null critical fields.",
)
def cleanse_vendor_data(vendor_file: Path) -> pd.DataFrame:
    """
    Loads the vendor CSV, removes rows where ``product_id``, ``quantity`` or
    ``price`` are null, and returns the cleaned DataFrame.
    """
    df = pd.read_csv(vendor_file)
    required_columns = ["product_id", "quantity", "price"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in vendor data: {missing}")

    cleaned_df = df.dropna(subset=required_columns)
    return cleaned_df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Merge cleansed vendor data with internal inventory.",
)
def merge_with_internal_inventory(
    vendor_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reads the internal inventory CSV, merges it with the vendor DataFrame on
    ``product_id``, updates stock levels and pricing, and writes the result to
    ``MERGED_OUTPUT_PATH``.
    """
    if not Path(INTERNAL_INVENTORY_PATH).exists():
        raise FileNotFoundError(
            f"Internal inventory file not found at {INTERNAL_INVENTORY_PATH}"
        )
    internal_df = pd.read_csv(INTERNAL_INVENTORY_PATH)

    if "product_id" not in internal_df.columns:
        raise ValueError("Internal inventory missing 'product_id' column.")

    merged = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )

    # Example logic: update quantity and price if vendor provides them
    merged["quantity"] = merged.apply(
        lambda row: row["quantity_vendor"]
        if not pd.isna(row["quantity_vendor"])
        else row["quantity_internal"],
        axis=1,
    )
    merged["price"] = merged.apply(
        lambda row: row["price_vendor"]
        if not pd.isna(row["price_vendor"])
        else row["price_internal"],
        axis=1,
    )

    # Drop intermediate columns
    cols_to_drop = [
        col
        for col in merged.columns
        if col.endswith("_vendor") or col.endswith("_internal")
    ]
    merged.drop(columns=cols_to_drop, inplace=True)

    merged.to_csv(MERGED_OUTPUT_PATH, index=False)
    return merged


@flow(description="Sensor‑gated pipeline for vendor inventory processing.")
def vendor_inventory_pipeline() -> Any:
    """
    Orchestrates the linear pipeline:
    1. Wait for the vendor file on FTP.
    2. Download the file.
    3. Cleanse the data.
    4. Merge with internal inventory.
    """
    filename = wait_for_ftp_file()
    vendor_path = download_vendor_file(filename)
    cleaned_df = cleanse_vendor_data(vendor_path)
    merged_df = merge_with_internal_inventory(cleaned_df)
    return merged_df


# The pipeline is intended to run daily; deployment configuration with a schedule
# should be added in the Prefect UI or via infrastructure code.

if __name__ == "__main__":
    vendor_inventory_pipeline()