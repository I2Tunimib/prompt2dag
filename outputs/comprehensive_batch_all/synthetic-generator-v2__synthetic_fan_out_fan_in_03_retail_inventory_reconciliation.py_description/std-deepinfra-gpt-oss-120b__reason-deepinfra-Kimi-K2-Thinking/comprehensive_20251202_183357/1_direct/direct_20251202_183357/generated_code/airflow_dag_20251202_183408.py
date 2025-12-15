from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_warehouse(warehouse: str, **kwargs):
    """
    Simulate fetching a CSV file for a given warehouse.
    The function creates a dummy CSV file in the /tmp directory and
    returns its path via XCom.
    """
    file_path = f"/tmp/{warehouse}_inventory.csv"
    # Create a simple CSV with header and dummy rows
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("sku,quantity\n")
        for i in range(1, 1001):
            f.write(f"{warehouse.upper()}-SKU-{i:04d},{i % 10 + 1}\n")
    return file_path


def normalize_warehouse(warehouse: str, **kwargs):
    """
    Simulate normalization of SKU formats for a warehouse.
    Reads the fetched CSV, applies transformations, writes a normalized CSV,
    and returns its path via XCom.
    """
    ti = kwargs["ti"]
    fetched_path = ti.xcom_pull(task_ids=f"fetch_{warehouse}")
    normalized_path = f"/tmp/{warehouse}_inventory_normalized.csv"

    with open(fetched_path, "r", encoding="utf-8") as src, open(
        normalized_path, "w", encoding="utf-8"
    ) as dst:
        header = src.readline()
        dst.write(header)  # keep header unchanged
        for line in src:
            sku, qty = line.strip().split(",")
            # Apply transformations: uppercase, strip whitespace, standard prefix
            sku = sku.upper().replace(" ", "")
            if not sku.startswith("SKU-"):
                sku = f"SKU-{sku.split('-')[-1]}"
            dst.write(f"{sku},{qty}\n")
    return normalized_path


def reconcile_inventory(**kwargs):
    """
    Reconcile inventory discrepancies across all normalized warehouse files.
    Generates a JSON discrepancy report and returns its path via XCom.
    """
    ti = kwargs["ti"]
    normalized_files = [
        ti.xcom_pull(task_ids=f"normalize_{wh}") for wh in ["north", "south", "east", "west"]
    ]

    sku_counts = {}
    for file_path in normalized_files:
        with open(file_path, "r", encoding="utf-8") as f:
            next(f)  # skip header
            for line in f:
                sku, qty = line.strip().split(",")
                qty = int(qty)
                sku_counts.setdefault(sku, []).append(qty)

    discrepancies = {}
    for sku, quantities in sku_counts.items():
        if len(set(quantities)) > 1:
            discrepancies[sku] = quantities

    report_path = "/tmp/discrepancy_report.json"
    with open(report_path, "w", encoding="utf-8") as rpt:
        json.dump(discrepancies, rpt, indent=2)

    return report_path


def generate_final_report(**kwargs):
    """
    Generate a final PDF reconciliation report.
    For demonstration, creates a simple text file representing the PDF.
    """
    ti = kwargs["ti"]
    discrepancy_path = ti.xcom_pull(task_ids="reconcile_inventory")
    final_report_path = "/tmp/final_reconciliation_report.pdf"

    # Simulate PDF creation by writing plain text
    with open(final_report_path, "w", encoding="utf-8") as pdf:
        pdf.write("Retail Inventory Reconciliation Report\n")
        pdf.write("=" * 40 + "\n\n")
        pdf.write(f"Discrepancy data source: {discrepancy_path}\n")
        pdf.write("Report generated on: " + datetime.utcnow().isoformat() + "\n")
        pdf.write("\n(Actual PDF generation logic would be implemented here.)\n")

    return final_report_path


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "owner": "airflow",
}

with DAG(
    dag_id="retail_inventory_reconciliation",
    default_args=default_args,
    description="Fan‑out/fan‑in pipeline to reconcile inventory across four warehouses",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
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

    # Normalization tasks (parallel, depend on all fetches)
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

    # Reconciliation task (single)
    reconcile = PythonOperator(
        task_id="reconcile_inventory",
        python_callable=reconcile_inventory,
    )

    # Final report generation (single)
    final_report = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
    )

    # Define dependencies
    [fetch_north, fetch_south, fetch_east, fetch_west] >> [
        normalize_north,
        normalize_south,
        normalize_east,
        normalize_west,
    ]
    [normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile
    reconcile >> final_report