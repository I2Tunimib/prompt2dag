from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def fetch_warehouse_csv(warehouse):
    """Simulate fetching CSV file from a warehouse."""
    print(f"Fetching CSV file from {warehouse} warehouse.")
    return f"{warehouse}_inventory.csv"

def normalize_sku_format(warehouse):
    """Simulate normalizing SKU formats for a warehouse."""
    print(f"Normalizing SKU formats for {warehouse} warehouse.")
    return f"{warehouse}_normalized.csv"

def reconcile_inventory(files):
    """Simulate reconciling inventory discrepancies across warehouses."""
    print("Reconciling inventory discrepancies.")
    discrepancy_report = "discrepancy_report.csv"
    with open(discrepancy_report, 'w') as f:
        f.write("SKU,Quantity_Variance\n")
        for _ in range(23):
            f.write(f"SKU{_},10\n")
    return discrepancy_report

def generate_reconciliation_report(discrepancy_report):
    """Simulate generating the final reconciliation report."""
    print("Generating final reconciliation report.")
    final_report = "final_reconciliation_report.pdf"
    with open(final_report, 'w') as f:
        f.write("Final Reconciliation Report\n")
        f.write("SKU,Quantity_Variance,Recommendations\n")
        with open(discrepancy_report, 'r') as dr:
            for line in dr:
                sku, variance = line.strip().split(',')
                f.write(f"{sku},{variance},Investigate and correct discrepancy\n")
    return final_report

with DAG(
    'retail_inventory_reconciliation',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_north = PythonOperator(
        task_id='fetch_north_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'north'},
    )

    fetch_south = PythonOperator(
        task_id='fetch_south_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'south'},
    )

    fetch_east = PythonOperator(
        task_id='fetch_east_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'east'},
    )

    fetch_west = PythonOperator(
        task_id='fetch_west_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'west'},
    )

    normalize_north = PythonOperator(
        task_id='normalize_north_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse': 'north'},
    )

    normalize_south = PythonOperator(
        task_id='normalize_south_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse': 'south'},
    )

    normalize_east = PythonOperator(
        task_id='normalize_east_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse': 'east'},
    )

    normalize_west = PythonOperator(
        task_id='normalize_west_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse': 'west'},
    )

    reconcile_inventory_task = PythonOperator(
        task_id='reconcile_inventory',
        python_callable=reconcile_inventory,
        op_kwargs={'files': [
            "{{ ti.xcom_pull(task_ids='normalize_north_sku') }}",
            "{{ ti.xcom_pull(task_ids='normalize_south_sku') }}",
            "{{ ti.xcom_pull(task_ids='normalize_east_sku') }}",
            "{{ ti.xcom_pull(task_ids='normalize_west_sku') }}"
        ]},
    )

    generate_report = PythonOperator(
        task_id='generate_reconciliation_report',
        python_callable=generate_reconciliation_report,
        op_kwargs={'discrepancy_report': "{{ ti.xcom_pull(task_ids='reconcile_inventory') }}"},
    )

    [fetch_north, fetch_south, fetch_east, fetch_west] >> [normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile_inventory_task >> generate_report