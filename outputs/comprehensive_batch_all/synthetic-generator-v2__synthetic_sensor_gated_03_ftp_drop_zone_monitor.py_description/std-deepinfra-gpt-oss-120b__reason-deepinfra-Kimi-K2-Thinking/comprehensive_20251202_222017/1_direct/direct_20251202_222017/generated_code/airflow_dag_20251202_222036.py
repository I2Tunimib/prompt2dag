from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.sensors.ftp import FTPFileSensor
from airflow.providers.ftp.hooks.ftp import FTPHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def download_vendor_file(**context):
    """
    Download vendor_inventory.csv from the FTP server to a temporary directory.
    The local file path is pushed to XCom for downstream tasks.
    """
    import logging
    import os
    import tempfile

    ftp_hook = FTPHook(ftp_conn_id="ftp_default")
    local_path = os.path.join(tempfile.gettempdir(), "vendor_inventory.csv")
    ftp_hook.retrieve_file("vendor_inventory.csv", local_path)
    logging.info("Downloaded vendor file to %s", local_path)
    context["ti"].xcom_push(key="vendor_file_path", value=local_path)


def cleanse_vendor_data(**context):
    """
    Remove rows with null values in product_id, quantity, or price.
    The cleansed file path is pushed to XCom for the merge step.
    """
    import logging

    import pandas as pd

    ti = context["ti"]
    file_path = ti.xcom_pull(key="vendor_file_path", task_ids="download_vendor_file")
    df = pd.read_csv(file_path)
    original_count = len(df)
    df = df.dropna(subset=["product_id", "quantity", "price"])
    cleaned_count = len(df)
    logging.info(
        "Cleansed vendor data: removed %d rows with nulls",
        original_count - cleaned_count,
    )
    clean_path = file_path.replace(".csv", "_clean.csv")
    df.to_csv(clean_path, index=False)
    ti.xcom_push(key="clean_vendor_file_path", value=clean_path)


def merge_with_internal_inventory(**context):
    """
    Merge the cleansed vendor data with internal inventory data on product_id.
    Updates stock levels and pricing, then writes the merged result to a CSV file.
    """
    import logging

    import pandas as pd

    ti = context["ti"]
    clean_path = ti.xcom_pull(
        key="clean_vendor_file_path", task_ids="cleanse_vendor_data"
    )
    vendor_df = pd.read_csv(clean_path)

    # Example internal inventory data; replace with real source as needed.
    internal_df = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "stock_level": [100, 200, 150],
            "price": [10.0, 20.0, 15.0],
        }
    )

    merged = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )
    merged["stock_level"] = merged["stock_level"] + merged["quantity"].fillna(0)
    merged["price"] = merged["price_vendor"].combine_first(merged["price_internal"])

    result_path = clean_path.replace("_clean.csv", "_merged.csv")
    merged.to_csv(result_path, index=False)
    logging.info("Merged inventory saved to %s", result_path)


with DAG(
    dag_id="ftp_vendor_inventory_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ftp", "inventory"],
) as dag:
    wait_for_ftp_file = FTPFileSensor(
        task_id="wait_for_ftp_file",
        ftp_conn_id="ftp_default",
        path=".",
        filename="vendor_inventory.csv",
        poke_interval=30,
        timeout=300,
    )

    download_task = PythonOperator(
        task_id="download_vendor_file",
        python_callable=download_vendor_file,
    )

    cleanse_task = PythonOperator(
        task_id="cleanse_vendor_data",
        python_callable=cleanse_vendor_data,
    )

    merge_task = PythonOperator(
        task_id="merge_with_internal_inventory",
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_task >> cleanse_task >> merge_task