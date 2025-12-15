from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_warehouse(warehouse: str, **kwargs) -> str:
    """
    Simulate fetching a CSV file from a warehouse system.

    The function creates a placeholder CSV file in the /tmp directory
    and returns its path.

    :param warehouse: Name of the warehouse (north, south, east, west).
    :return: Path to the fetched raw CSV file.
    """
    raw_path = Path(f"/tmp/{warehouse}_raw.csv")
    raw_path.write_text("sku,quantity\n")
    return str(raw_path)


def normalize_warehouse(warehouse: str, **kwargs) -> str:
    """
    Simulate normalizing SKU formats for a warehouse.

    Reads the raw CSV path from XCom, applies mock transformations,
    writes a normalized CSV file, and returns its path.

    :param warehouse: Name of the warehouse.
    :return: Path to the normalized CSV file.
    """
    ti = kwargs["ti"]
    raw_path = ti.xcom_pull(task_ids=f"fetch_{warehouse}")
    if not raw_path:
        raise ValueError(f"Raw file for {warehouse} not found in XCom.")
    norm_path = Path(f"/tmp/{warehouse}_norm.csv")
    # Mock transformation: just copy header for demonstration.
    norm_path.write_text("sku,quantity\n")
    return str(norm_path)


def reconcile(**kwargs) -> str:
    """
    Reconcile inventory discrepancies across all warehouses.

    Pulls normalized CSV paths from XCom, performs a mock comparison,
    writes a discrepancy report, and returns its path.

    :return: Path to the discrepancy report CSV file.
    """
    ti = kwargs["ti"]
    warehouses = ["north", "south", "east", "west"]
    norm_paths = [
        ti.xcom_pull(task_ids=f"normalize_{wh}") for wh in warehouses
    ]
    if not all(norm_paths):
        missing = [wh for wh, p in zip(warehouses, norm_paths) if not p]
        raise ValueError(f"Missing normalized files for: {missing}")

    discrepancy_path = Path("/tmp/discrepancy_report.csv")
    discrepancy_path.write_text("sku,qty_north,qty_south,qty_east,qty_west\n")
    return str(discrepancy_path)


def generate_report(**kwargs) -> str:
    """
    Generate the final PDF reconciliation report.

    Reads the discrepancy report path from XCom, creates a placeholder PDF,
    and returns its path.

    :return: Path to the final PDF report.
    """
    ti = kwargs["ti"]
    discrepancy_path = ti.xcom_pull(task_ids="reconcile")
    if not discrepancy_path:
        raise ValueError("Discrepancy report not found in XCom.")

    report_path = Path("/tmp/final_reconciliation_report.pdf")
    report_path.write_text("PDF content with reconciliation details")
    return str(report_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_inventory_reconciliation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_tasks=4,
    tags=["retail", "inventory", "reconciliation"],
) as dag:
    # Fetch tasks (parallel)
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

    # Normalization tasks (parallel)
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
    reconcile_task = PythonOperator(
        task_id="reconcile",
        python_callable=reconcile,
    )

    # Final report generation task
    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Define dependencies
    fetch_north >> normalize_north
    fetch_south >> normalize_south
    fetch_east >> normalize_east
    fetch_west >> normalize_west

    (
        normalize_north,
        normalize_south,
        normalize_east,
        normalize_west,
    ) >> reconcile_task

    reconcile_task >> generate_report_task