# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for wait_for_ftp_file_pipeline
Author: Auto-generated
Date: 2024-06-28
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from ftplib import FTP, error_perm

import pendulum
from airflow import DAG
from airflow.decorators import task, dag as dag_decorator
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.hooks.base import BaseHook

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_ftp_connection(conn_id: str) -> FTP:
    """
    Retrieve FTP connection details from Airflow and return an active FTP client.
    """
    try:
        conn: Connection = BaseHook.get_connection(conn_id)
        ftp = FTP()
        ftp.connect(host=conn.host, port=conn.port or 21, timeout=30)
        ftp.login(user=conn.login, passwd=conn.password)
        logging.info("Connected to FTP server %s", conn.host)
        return ftp
    except Exception as exc:
        raise AirflowException(f"Failed to connect to FTP server: {exc}") from exc


def get_local_tmp_path() -> Path:
    """
    Return a temporary directory path for storing intermediate files.
    """
    tmp_dir = Path(tempfile.gettempdir()) / "wait_for_ftp_file_pipeline"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return tmp_dir


@dag_decorator(
    dag_id="wait_for_ftp_file_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ftp", "etl"],
    doc_md="## wait_for_ftp_file_pipeline\nNo description provided.",
)
def pipeline():
    """
    Sequential pipeline that waits for a file on an FTP server,
    downloads it, cleanses the data, and merges it with the internal inventory.
    """

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def wait_for_ftp_file(file_name: str = "vendor_data.csv", timeout_minutes: int = 30) -> str:
        """
        Poll the FTP server until the expected file appears or timeout is reached.
        Returns the absolute path of the file on the FTP server.
        """
        ftp = get_ftp_connection("ftp_server")
        deadline = datetime.utcnow() + timedelta(minutes=timeout_minutes)

        while datetime.utcnow() < deadline:
            try:
                files = ftp.nlst()
                if file_name in files:
                    logging.info("File %s found on FTP server.", file_name)
                    ftp.quit()
                    return file_name
                else:
                    logging.info("File %s not yet present. Sleeping 60s.", file_name)
                    time.sleep(60)
            except error_perm as e:
                ftp.quit()
                raise AirflowException(f"FTP permission error while listing files: {e}") from e
            except Exception as e:
                ftp.quit()
                raise AirflowException(f"Unexpected error while checking FTP: {e}") from e

        raise AirflowException(f"Timeout reached while waiting for {file_name} on FTP server.")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def download_vendor_file(ftp_file_name: str) -> str:
        """
        Download the specified file from the FTP server to a local temporary directory.
        Returns the local file path.
        """
        ftp = get_ftp_connection("ftp_server")
        local_dir = get_local_tmp_path()
        local_path = local_dir / ftp_file_name

        try:
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {ftp_file_name}", f.write)
            logging.info("Downloaded %s to %s", ftp_file_name, local_path)
        except Exception as exc:
            raise AirflowException(f"Failed to download {ftp_file_name}: {exc}") from exc
        finally:
            ftp.quit()

        return str(local_path)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def cleanse_vendor_data(local_file_path: str) -> str:
        """
        Perform basic cleansing on the downloaded CSV file.
        Returns the path to the cleansed file.
        """
        import pandas as pd

        try:
            df = pd.read_csv(local_file_path)
            logging.info("Read %d rows from %s", len(df), local_file_path)

            # Example cleansing steps
            df.dropna(subset=["product_id", "quantity"], inplace=True)
            df["quantity"] = df["quantity"].astype(int)
            df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

            cleansed_path = Path(local_file_path).with_name("cleansed_" + Path(local_file_path).name)
            df.to_csv(cleansed_path, index=False)
            logging.info("Cleansed data written to %s", cleansed_path)
        except Exception as exc:
            raise AirflowException(f"Data cleansing failed for {local_file_path}: {exc}") from exc

        return str(cleansed_path)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def merge_with_internal_inventory(cleansed_file_path: str) -> None:
        """
        Merge the cleansed vendor data with the internal inventory database.
        """
        import pandas as pd
        from sqlalchemy import create_engine, text

        # Retrieve DB connection from Airflow
        try:
            conn: Connection = BaseHook.get_connection("internal_inventory_db")
            db_url = f"{conn.get_uri()}"
            engine = create_engine(db_url)
        except Exception as exc:
            raise AirflowException(f"Failed to obtain DB connection: {exc}") from exc

        try:
            vendor_df = pd.read_csv(cleansed_file_path)
            logging.info("Merging %d vendor records with internal inventory.", len(vendor_df))

            with engine.begin() as conn:
                # Example: upsert logic using PostgreSQL syntax
                for _, row in vendor_df.iterrows():
                    upsert_stmt = text(
                        """
                        INSERT INTO inventory (product_id, quantity, price, source)
                        VALUES (:product_id, :quantity, :price, 'vendor')
                        ON CONFLICT (product_id) DO UPDATE
                        SET quantity = EXCLUDED.quantity,
                            price = EXCLUDED.price,
                            source = EXCLUDED.source;
                        """
                    )
                    conn.execute(
                        upsert_stmt,
                        {
                            "product_id": row["product_id"],
                            "quantity": row["quantity"],
                            "price": row["price"],
                        },
                    )
            logging.info("Merge completed successfully.")
        except Exception as exc:
            raise AirflowException(f"Failed during merge operation: {exc}") from exc

    # Define task flow
    ftp_file = wait_for_ftp_file()
    downloaded_path = download_vendor_file(ftp_file)
    cleansed_path = cleanse_vendor_data(downloaded_path)
    merge_with_internal_inventory(cleansed_path)


# Instantiate the DAG
dag = pipeline()