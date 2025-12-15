import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.sensors.ftp import FTPFileSensor
from airflow.hooks.ftp_hook import FTPHook

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# Paths used in the pipeline
LOCAL_TMP_DIR = "/tmp"
VENDOR_FILENAME = "vendor_inventory.csv"
VENDOR_FILEPATH = os.path.join(LOCAL_TMP_DIR, VENDOR_FILENAME)
CLEAN_VENDOR_FILENAME = "vendor_inventory_clean.csv"
CLEAN_VENDOR_FILEPATH = os.path.join(LOCAL_TMP_DIR, CLEAN_VENDOR_FILENAME)
MERGED_FILENAME = "merged_inventory.csv"
MERGED_FILEPATH = os.path.join(LOCAL_TMP_DIR, MERGED_FILENAME)
INTERNAL_INVENTORY_PATH = "/path/to/internal_inventory.csv"  # Adjust as needed

def download_vendor_file(**kwargs):
    """
    Downloads the vendor inventory file from the FTP server to a local temporary directory.
    """
    ftp_conn_id = "ftp_default"
    ftp_hook = FTPHook(ftp_conn_id=ftp_conn_id)
    logging.info("Downloading %s from FTP server to %s", VENDOR_FILENAME, VENDOR_FILEPATH)
    ftp_hook.retrieve_file(VENDOR_FILENAME, VENDOR_FILEPATH)
    # Push the local path to XCom for downstream tasks
    kwargs["ti"].xcom_push(key="vendor_file_path", value=VENDOR_FILEPATH)

def cleanse_vendor_data(**kwargs):
    """
    Cleanses the downloaded vendor data by removing rows with null values
    in product_id, quantity, or price columns.
    """
    ti = kwargs["ti"]
    vendor_path = ti.xcom_pull(key="vendor_file_path", task_ids="download_vendor_file")
    if not vendor_path or not os.path.exists(vendor_path):
        raise FileNotFoundError(f"Vendor file not found at {vendor_path}")

    df = pd.read_csv(vendor_path)
    logging.info("Original vendor data shape: %s", df.shape)
    df_clean = df.dropna(subset=["product_id", "quantity", "price"])
    logging.info("Cleansed vendor data shape: %s", df_clean.shape)
    df_clean.to_csv(CLEAN_VENDOR_FILEPATH, index=False)
    ti.xcom_push(key="clean_vendor_file_path", value=CLEAN_VENDOR_FILEPATH)

def merge_with_internal_inventory(**kwargs):
    """
    Merges the cleansed vendor data with the internal inventory data on product_id,
    updating stock levels and pricing.
    """
    ti = kwargs["ti"]
    clean_path = ti.xcom_pull(key="clean_vendor_file_path", task_ids="cleanse_vendor_data")
    if not clean_path or not os.path.exists(clean_path):
        raise FileNotFoundError(f"Cleansed vendor file not found at {clean_path}")

    if not os.path.exists(INTERNAL_INVENTORY_PATH):
        raise FileNotFoundError(f"Internal inventory file not found at {INTERNAL_INVENTORY_PATH}")

    vendor_df = pd.read_csv(clean_path)
    internal_df = pd.read_csv(INTERNAL_INVENTORY_PATH)

    logging.info("Merging internal (%s rows) with vendor (%s rows)", internal_df.shape[0], vendor_df.shape[0])
    merged = internal_df.merge(
        vendor_df[["product_id", "quantity", "price"]],
        on="product_id",
        how="left",
        suffixes=("_int", "_vendor"),
    )

    # Update stock levels
    merged["stock"] = merged["stock"] + merged["quantity"].fillna(0)

    # Update price: vendor price overrides internal price when present
    merged["price"] = merged["price_vendor"].combine_first(merged["price_int"])

    merged.to_csv(MERGED_FILEPATH, index=False)
    logging.info("Merged inventory saved to %s", MERGED_FILEPATH)

with DAG(
    dag_id="ftp_vendor_inventory_pipeline",
    default_args=default_args,
    description="Sensor‑gated pipeline that downloads, cleanses, and merges vendor inventory data.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ftp", "inventory"],
) as dag:
    wait_for_ftp_file = FTPFileSensor(
        task_id="wait_for_ftp_file",
        ftp_conn_id="ftp_default",
        path="/",
        filename=VENDOR_FILENAME,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    download_task = PythonOperator(
        task_id="download_vendor_file",
        python_callable=download_vendor_file,
        provide_context=True,
    )

    cleanse_task = PythonOperator(
        task_id="cleanse_vendor_data",
        python_callable=cleanse_vendor_data,
        provide_context=True,
    )

    merge_task = PythonOperator(
        task_id="merge_with_internal_inventory",
        python_callable=merge_with_internal_inventory,
        provide_context=True,
    )

    # Define linear dependencies
    wait_for_ftp_file >> download_task >> cleanse_task >> merge_task