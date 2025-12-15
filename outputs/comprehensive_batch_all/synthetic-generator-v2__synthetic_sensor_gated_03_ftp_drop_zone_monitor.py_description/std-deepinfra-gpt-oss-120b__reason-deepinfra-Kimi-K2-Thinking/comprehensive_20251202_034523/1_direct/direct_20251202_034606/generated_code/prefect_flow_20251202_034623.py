import os
import time
import tempfile
from ftplib import FTP, error_perm
from typing import Any

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


FTP_HOST = os.getenv("FTP_HOST", "localhost")
FTP_USER = os.getenv("FTP_USER", "anonymous")
FTP_PASS = os.getenv("FTP_PASS", "")
VENDOR_FILE_NAME = "vendor_inventory.csv"
POKE_INTERVAL = 30  # seconds
TIMEOUT = 300  # seconds (5 minutes)
MAX_ATTEMPTS = TIMEOUT // POKE_INTERVAL
INTERNAL_INVENTORY_PATH = os.getenv(
    "INTERNAL_INVENTORY_PATH", "internal_inventory.csv"
)


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def wait_for_ftp_file() -> str:
    """
    Poll the FTP server until the vendor inventory file is present.

    Returns:
        The name of the detected file (always VENDOR_FILE_NAME).

    Raises:
        FileNotFoundError: If the file is not found within the timeout period.
    """
    logger = get_run_logger()
    logger.info("Starting FTP sensor for %s", VENDOR_FILE_NAME)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with FTP(FTP_HOST) as ftp:
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                files = ftp.nlst()
                if VENDOR_FILE_NAME in files:
                    logger.info(
                        "File %s detected on attempt %d", VENDOR_FILE_NAME, attempt
                    )
                    return VENDOR_FILE_NAME
                else:
                    logger.debug(
                        "Attempt %d: %s not found. Retrying in %d seconds.",
                        attempt,
                        VENDOR_FILE_NAME,
                        POKE_INTERVAL,
                    )
        except Exception as exc:
            logger.error("FTP connection error on attempt %d: %s", attempt, exc)

        time.sleep(POKE_INTERVAL)

    raise FileNotFoundError(
        f"File {VENDOR_FILE_NAME} not found on FTP server after {TIMEOUT} seconds."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def download_vendor_file(file_name: str) -> str:
    """
    Download the vendor file from the FTP server to a temporary location.

    Args:
        file_name: Name of the file to download.

    Returns:
        Path to the downloaded local file.
    """
    logger = get_run_logger()
    logger.info("Downloading %s from FTP server", file_name)

    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            local_path = tmp_file.name

            def _write_chunk(chunk: bytes):
                tmp_file.write(chunk)

            try:
                ftp.retrbinary(f"RETR {file_name}", _write_chunk)
                logger.info("File downloaded to %s", local_path)
            except error_perm as e:
                logger.error("Permission error while downloading: %s", e)
                raise FileNotFoundError(f"Unable to download {file_name}") from e

    return local_path


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def cleanse_vendor_data(local_path: str) -> pd.DataFrame:
    """
    Cleanse the vendor CSV by dropping rows with null values in critical columns.

    Args:
        local_path: Path to the downloaded vendor CSV file.

    Returns:
        A pandas DataFrame containing the cleansed data.
    """
    logger = get_run_logger()
    logger.info("Reading vendor data from %s", local_path)

    df = pd.read_csv(local_path)

    required_columns = ["product_id", "quantity", "price"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in vendor data: {missing}")

    initial_count = len(df)
    df_clean = df.dropna(subset=required_columns)
    final_count = len(df_clean)

    logger.info(
        "Cleansed vendor data: dropped %d rows with null values", initial_count - final_count
    )
    return df_clean


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def merge_with_internal_inventory(vendor_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the cleansed vendor data with the internal inventory.

    Args:
        vendor_df: DataFrame containing cleansed vendor data.

    Returns:
        A DataFrame representing the merged inventory.
    """
    logger = get_run_logger()
    logger.info("Loading internal inventory from %s", INTERNAL_INVENTORY_PATH)

    if not os.path.exists(INTERNAL_INVENTORY_PATH):
        raise FileNotFoundError(
            f"Internal inventory file not found at {INTERNAL_INVENTORY_PATH}"
        )

    internal_df = pd.read_csv(INTERNAL_INVENTORY_PATH)

    if "product_id" not in internal_df.columns:
        raise ValueError("Internal inventory must contain a 'product_id' column")

    merged_df = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )

    # Update stock levels and pricing where vendor data is present
    merged_df["quantity"] = merged_df.apply(
        lambda row: row["quantity_vendor"]
        if not pd.isna(row["quantity_vendor"])
        else row["quantity_internal"],
        axis=1,
    )
    merged_df["price"] = merged_df.apply(
        lambda row: row["price_vendor"]
        if not pd.isna(row["price_vendor"])
        else row["price_internal"],
        axis=1,
    )

    # Drop intermediate columns
    cols_to_drop = [
        col
        for col in merged_df.columns
        if col.endswith("_internal") or col.endswith("_vendor")
    ]
    merged_df = merged_df.drop(columns=cols_to_drop)

    logger.info("Merged inventory contains %d records", len(merged_df))
    return merged_df


@flow
def vendor_inventory_pipeline() -> pd.DataFrame:
    """
    Orchestrates the vendor inventory processing pipeline.
    """
    logger = get_run_logger()
    logger.info("Starting vendor inventory pipeline")

    file_name = wait_for_ftp_file()
    local_path = download_vendor_file(file_name)
    cleaned_df = cleanse_vendor_data(local_path)
    final_df = merge_with_internal_inventory(cleaned_df)

    logger.info("Pipeline completed successfully")
    return final_df


# Note: In a production deployment, configure a Prefect deployment with a daily schedule.
if __name__ == "__main__":
    result = vendor_inventory_pipeline()
    print("Final merged inventory preview:")
    print(result.head())