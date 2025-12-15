import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_warehouse(warehouse: str, **kwargs) -> str:
    """
    Simulate fetching a CSV file from a warehouse system.

    :param warehouse: Name of the warehouse (north, south, east, west).
    :return: Path to the fetched raw CSV file.
    """
    file_path = f"/tmp/{warehouse}_raw.csv"
    logging.info("Fetched data for warehouse %s into %s", warehouse, file_path)
    # In a real implementation, download the file here.
    return file_path


def normalize_warehouse(warehouse: str, **kwargs) -> str:
    """
    Simulate normalizing SKU formats for a warehouse.

    :param warehouse: Name of the warehouse.
    :return: Path to the normalized CSV file.
    """
    ti = kwargs["ti"]
    raw_path = ti.xcom_pull(task_ids=f"fetch_{warehouse}")
    normalized_path = f"/tmp/{warehouse}_normalized.csv"
    logging.info(
        "Normalizing data for warehouse %s: raw=%s -> normalized=%s",
        warehouse,
        raw_path,
        normalized_path,
    )
    # In a real implementation, read raw_path, transform SKUs, write normalized_path.
    return normalized_path


def reconcile_inventory(**kwargs) -> str:
    """
    Compare normalized inventories from all warehouses and generate a discrepancy report.

    :return: Path to the discrepancy report CSV file.
    """
    ti = kwargs["ti"]
    normalized_paths = ti.xcom_pull(
        task_ids=[
            "normalize_north",
            "normalize_south",
            "normalize_east",
            "normalize_west",
        ]
    )
    report_path = "/tmp/discrepancy_report.csv"
    logging.info(
        "Reconciling inventories from %s and generating report %s",
        normalized_paths,
        report_path,
    )
    # In a real implementation, load each normalized file, compare SKUs, write report.
    return report_path


def generate_final_report(**kwargs) -> str:
    """
    Create a comprehensive PDF report based on the discrepancy data.

    :return: Path to the final PDF report.
    """
    ti = kwargs["ti"]
    discrepancy_path = ti.xcom_pull(task_ids="reconcile_inventory")
    pdf_path = "/tmp/final_reconciliation_report.pdf"
    logging.info(
        "Generating final PDF report %s from discrepancy data %s",
        pdf_path,
        discrepancy_path,
    )
    # In a real implementation, read the CSV, format PDF, and write to pdf_path.
    return pdf_path


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="retail_inventory_reconciliation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_tasks=4,
    tags=["retail", "inventory", "reconciliation"],
) as dag:
    # Fetch tasks
    fetch_north = PythonOperator(
        task_id="fetch_north",
        python_callable=fetch_warehouse,
        op_kwargs={"warehouse": "north"},
    )
    fetch_south = PythonOperator(
        task_id="fetch_south",
        python_callable=fetch_warehouse,
        op_kwargs={"warehouse": "south"},
    )
    fetch_east = PythonOperator(
        task_id="fetch_east",
        python_callable=fetch_warehouse,
        op_kwargs={"warehouse": "east"},
    )
    fetch_west = PythonOperator(
        task_id="fetch_west",
        python_callable=fetch_warehouse,
        op_kwargs={"warehouse": "west"},
    )

    # Normalization tasks
    normalize_north = PythonOperator(
        task_id="normalize_north",
        python_callable=normalize_warehouse,
        op_kwargs={"warehouse": "north"},
    )
    normalize_south = PythonOperator(
        task_id="normalize_south",
        python_callable=normalize_warehouse,
        op_kwargs={"warehouse": "south"},
    )
    normalize_east = PythonOperator(
        task_id="normalize_east",
        python_callable=normalize_warehouse,
        op_kwargs={"warehouse": "east"},
    )
    normalize_west = PythonOperator(
        task_id="normalize_west",
        python_callable=normalize_warehouse,
        op_kwargs={"warehouse": "west"},
    )

    # Reconciliation task
    reconcile = PythonOperator(
        task_id="reconcile_inventory",
        python_callable=reconcile_inventory,
    )

    # Final report generation task
    generate_report = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
    )

    # Define dependencies
    fetch_north >> normalize_north
    fetch_south >> normalize_south
    fetch_east >> normalize_east
    fetch_west >> normalize_west

    [
        normalize_north,
        normalize_south,
        normalize_east,
        normalize_west,
    ] >> reconcile

    reconcile >> generate_report