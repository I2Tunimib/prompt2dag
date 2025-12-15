import time
from ftplib import FTP
from pathlib import Path
from typing import Dict

from prefect import flow, task
import pandas as pd


@task(
    name="wait_for_ftp_file",
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
def wait_for_ftp_file(
    ftp_host: str,
    ftp_user: str,
    ftp_password: str,
    ftp_path: str,
    filename: str = "vendor_inventory.csv",
    poke_interval: int = 30,
) -> bool:
    """
    Poll FTP server for file presence every 30 seconds.
    Returns True when file is found, raises TimeoutError if not found within 5 minutes.
    """
    start_time = time.time()
    timeout_seconds = 300

    while True:
        try:
            with FTP(ftp_host) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_password)
                ftp.cwd(ftp_path)
                files = ftp.nlst()
                if filename in files:
                    print(f"File '{filename}' found on FTP server.")
                    return True
        except Exception as e:
            print(f"FTP connection error: {e}")
            raise

        elapsed = time.time() - start_time
        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"File '{filename}' not found on FTP server after {timeout_seconds} seconds."
            )

        print(f"File not found. Waiting {poke_interval} seconds before next check...")
        time.sleep(poke_interval)


@task(
    name="download_vendor_file",
    retries=2,
    retry_delay_seconds=300,
)
def download_vendor_file(
    ftp_host: str,
    ftp_user: str,
    ftp_password: str,
    ftp_path: str,
    local_dir: str,
    filename: str = "vendor_inventory.csv",
) -> Path:
    """
    Download the detected file from FTP server to local temporary directory.
    """
    local_path = Path(local_dir) / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)
            ftp.cwd(ftp_path)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)

        print(f"File downloaded successfully to {local_path}")
        return local_path

    except Exception as e:
        print(f"Failed to download file: {e}")
        raise


@task(
    name="cleanse_vendor_data",
    retries=2,
    retry_delay_seconds=300,
)
def cleanse_vendor_data(file_path: Path) -> Path:
    """
    Cleanse vendor data by removing null values from product_id, quantity, and price columns.
    """
    try:
        df = pd.read_csv(file_path)
        initial_rows = len(df)
        cleansed_df = df.dropna(subset=['product_id', 'quantity', 'price'])
        final_rows = len(cleansed_df)

        print(f"Cleaned data: {initial_rows} -> {final_rows} rows "
              f"({initial_rows - final_rows} rows removed due to null values)")

        cleansed_path = file_path.parent / f"cleansed_{file_path.name}"
        cleansed_df.to_csv(cleansed_path, index=False)

        print(f"Cleansed data saved to {cleansed_path}")
        return cleansed_path

    except Exception as e:
        print(f"Failed to cleanse data: {e}")
        raise


@task(
    name="merge_with_internal_inventory",
    retries=2,
    retry_delay_seconds=300,
)
def merge_with_internal_inventory(cleansed_file_path: Path) -> Dict:
    """