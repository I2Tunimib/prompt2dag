import os
import time
import ftplib
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import MaxRetries, RetryDelay


# Configuration – replace with real credentials or environment variables
FTP_HOST = os.getenv("FTP_HOST", "ftp.example.com")
FTP_USER = os.getenv("FTP_USER", "anonymous")
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "")
VENDOR_FILENAME = "vendor_inventory.csv"
INTERNAL_INVENTORY_PATH = Path("internal_inventory.csv")
POKE_INTERVAL = 30  # seconds
TIMEOUT = 5 * 60  # 5 minutes in seconds


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Poll FTP server until the vendor file appears.",
)
def wait_for_ftp_file() -> bool:
    """
    Poll the FTP server every POKE_INTERVAL seconds until VENDOR_FILENAME is found
    or TIMEOUT seconds have elapsed.

    Returns:
        True if the file is detected within the timeout period.

    Raises:
        RuntimeError: If the file is not found within the timeout.
    """
    deadline = datetime.utcnow() + timedelta(seconds=TIMEOUT)
    while datetime.utcnow() < deadline:
        try:
            with ftplib.FTP(FTP_HOST) as ftp:
                ftp.login(FTP_USER, FTP_PASSWORD)
                files = ftp.nlst()
                if VENDOR_FILENAME in files:
                    return True
        except ftplib.all_errors as exc:
            # Log the exception and continue polling
            print(f"FTP error while checking for file: {exc}")
        time.sleep(POKE_INTERVAL)

    raise RuntimeError(
        f"File {VENDOR_FILENAME} not found on FTP server within {TIMEOUT} seconds."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Download the vendor CSV file from the FTP server.",
)
def download_vendor_file() -> Path:
    """
    Retrieve VENDOR_FILENAME from the FTP server and store it in a temporary directory.

    Returns:
        Path to the downloaded CSV file.
    """
    temp_dir = Path(tempfile.mkdtemp())
    local_path = temp_dir / VENDOR_FILENAME

    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login(FTP_USER, FTP_PASSWORD)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {VENDOR_FILENAME}", f.write)

    print(f"Downloaded vendor file to {local_path}")
    return local_path


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Cleanse the vendor data by dropping rows with null critical fields.",
)
def cleanse_vendor_data(vendor_file: Path) -> pd.DataFrame:
    """
    Load the vendor CSV, drop rows where product_id, quantity, or price are null.

    Args:
        vendor_file: Path to the raw vendor CSV file.

    Returns:
        A pandas DataFrame containing the cleansed data.
    """
    df = pd.read_csv(vendor_file)

    required_columns = ["product_id", "quantity", "price"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in vendor data: {missing}")

    cleaned_df = df.dropna(subset=required_columns)
    print(f"Cleaned vendor data: {len(df)} rows -> {len(cleaned_df)} rows")
    return cleaned_df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Merge cleansed vendor data with internal inventory.",
)
def merge_with_internal_inventory(
    cleaned_vendor_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge the vendor data with the internal inventory on product_id,
    updating stock levels and pricing.

    Args:
        cleaned_vendor_df: DataFrame with cleansed vendor data.

    Returns:
        DataFrame containing the merged inventory.
    """
    if INTERNAL_INVENTORY_PATH.is_file():
        internal_df = pd.read_csv(INTERNAL_INVENTORY_PATH)
    else:
        # Create an empty internal inventory if none exists
        internal_df = pd.DataFrame(columns=["product_id", "stock", "price"])
        print("Internal inventory file not found; using empty DataFrame.")

    if "product_id" not in internal_df.columns:
        internal_df["product_id"] = pd.Series(dtype=str)

    merged = pd.merge(
        internal_df,
        cleaned_vendor_df,
        on="product_id",
        how="outer",
        suffixes=("_internal", "_vendor"),
    )

    # Update stock: prefer vendor quantity if available, else keep internal stock
    merged["stock"] = merged["quantity"].combine_first(merged["stock"])
    # Update price: prefer vendor price if available, else keep internal price
    merged["price"] = merged["price_vendor"].combine_first(merged["price_internal"])

    # Drop intermediate columns
    merged = merged[["product_id", "stock", "price"]]

    # Optionally write back to the internal inventory file
    merged.to_csv(INTERNAL_INVENTORY_PATH, index=False)
    print(f"Merged inventory written to {INTERNAL_INVENTORY_PATH}")

    return merged


@flow(description="Sensor‑gated pipeline to process vendor inventory.")
def vendor_inventory_pipeline() -> None:
    """
    Orchestrates the linear pipeline:
    Wait for FTP file → Download → Cleanse → Merge.
    """
    # Step 1: Wait for the file to appear on the FTP server
    wait_for_ftp_file()

    # Step 2: Download the file
    vendor_path = download_vendor_file()

    # Step 3: Cleanse the data
    cleaned_df = cleanse_vendor_data(vendor_path)

    # Step 4: Merge with internal inventory
    merge_with_internal_inventory(cleaned_df)


# Note: In a production deployment, configure a daily schedule in the Prefect UI
# or via a deployment script referencing this flow.

if __name__ == "__main__":
    vendor_inventory_pipeline()