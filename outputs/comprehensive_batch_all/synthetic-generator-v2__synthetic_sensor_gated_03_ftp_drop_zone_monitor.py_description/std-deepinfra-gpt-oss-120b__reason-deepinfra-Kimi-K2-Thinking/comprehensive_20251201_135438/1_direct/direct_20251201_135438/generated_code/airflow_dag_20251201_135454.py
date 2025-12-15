from datetime import datetime, timedelta
from pathlib import Path
import logging
import ftplib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.sensors.ftp import FTPFileSensor


def download_vendor_file(**context):
    """
    Download the vendor_inventory.csv file from the FTP server
    and store it in a temporary local directory.
    """
    ftp_conn_id = "ftp_default"
    remote_path = "/vendor_inventory.csv"
    local_dir = Path("/tmp/airflow_vendor")
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / "vendor_inventory.csv"

    # Retrieve FTP connection details from Airflow connection
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(ftp_conn_id)
    host = conn.host
    login = conn.login
    password = conn.password
    port = conn.port or 21

    logging.info("Connecting to FTP %s:%s", host, port)
    with ftplib.FTP() as ftp:
        ftp.connect(host, port)
        ftp.login(login, password)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)

    logging.info("Downloaded file to %s", local_path)
    # Push path to XCom for downstream tasks
    context["ti"].xcom_push(key="vendor_file_path", value=str(local_path))


def cleanse_vendor_data(**context):
    """
    Cleanse the downloaded vendor data by removing rows with null
    values in product_id, quantity, or price columns.
    """
    import pandas as pd

    ti = context["ti"]
    file_path = ti.xcom_pull(key="vendor_file_path", task_ids="download_vendor_file")
    df = pd.read_csv(file_path)

    required_columns = ["product_id", "quantity", "price"]
    before_rows = len(df)
    df_clean = df.dropna(subset=required_columns)
    after_rows = len(df_clean)

    logging.info(
        "Cleansed data: removed %d rows with null values", before_rows - after_rows
    )

    clean_path = Path(file_path).with_name("vendor_inventory_clean.csv")
    df_clean.to_csv(clean_path, index=False)
    logging.info("Clean data saved to %s", clean_path)

    ti.xcom_push(key="clean_vendor_file_path", value=str(clean_path))


def merge_with_internal_inventory(**context):
    """
    Merge the cleansed vendor data with internal inventory data
    on product_id and update stock levels and pricing.
    """
    import pandas as pd

    ti = context["ti"]
    clean_path = ti.xcom_pull(
        key="clean_vendor_file_path", task_ids="cleanse_vendor_data"
    )
    vendor_df = pd.read_csv(clean_path)

    # Placeholder: load internal inventory data
    internal_path = "/tmp/airflow_vendor/internal_inventory.csv"
    internal_df = pd.read_csv(internal_path)

    merged_df = pd.merge(
        internal_df,
        vendor_df,
        on="product_id",
        how="left",
        suffixes=("_internal", "_vendor"),
    )

    # Update stock and price where vendor data is present
    merged_df["quantity"] = merged_df["quantity_vendor"].fillna(
        merged_df["quantity_internal"]
    )
    merged_df["price"] = merged_df["price_vendor"].fillna(merged_df["price_internal"])

    result_path = Path("/tmp/airflow_vendor/merged_inventory.csv")
    merged_df.to_csv(result_path, index=False)
    logging.info("Merged inventory saved to %s", result_path)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

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
        path="/",
        file_name="vendor_inventory.csv",
        poke_interval=30,
        timeout=300,
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

    wait_for_ftp_file >> download_task >> cleanse_task >> merge_task