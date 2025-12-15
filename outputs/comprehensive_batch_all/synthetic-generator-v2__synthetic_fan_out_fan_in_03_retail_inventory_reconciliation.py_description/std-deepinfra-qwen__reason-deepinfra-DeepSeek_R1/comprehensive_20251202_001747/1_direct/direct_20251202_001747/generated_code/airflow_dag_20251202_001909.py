from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Mock functions to simulate real operations
def fetch_warehouse_csv(warehouse):
    print(f"Fetching CSV from {warehouse} warehouse")
    # Simulate fetching CSV file
    return f"{warehouse}_inventory.csv"

def normalize_sku(warehouse):
    print(f"Normalizing SKU formats for {warehouse} warehouse")
    # Simulate SKU normalization
    return f"{warehouse}_normalized_inventory.csv"

def reconcile_inventory():
    print("Reconciling inventory discrepancies")
    # Simulate inventory reconciliation
    return "discrepancy_report.csv"

def generate_reconciliation_report():
    print("Generating final reconciliation report")
    # Simulate report generation
    return "final_reconciliation_report.pdf"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_inventory_reconciliation',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_north_csv = PythonOperator(
        task_id='fetch_north_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'north'},
    )

    fetch_south_csv = PythonOperator(
        task_id='fetch_south_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'south'},
    )

    fetch_east_csv = PythonOperator(
        task_id='fetch_east_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'east'},
    )

    fetch_west_csv = PythonOperator(
        task_id='fetch_west_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse': 'west'},
    )

    normalize_north_sku = PythonOperator(
        task_id='normalize_north_sku',
        python_callable=normalize_sku,
        op_kwargs={'warehouse': 'north'},
    )

    normalize_south_sku = PythonOperator(
        task_id='normalize_south_sku',
        python_callable=normalize_sku,
        op_kwargs={'warehouse': 'south'},
    )

    normalize_east_sku = PythonOperator(
        task_id='normalize_east_sku',
        python_callable=normalize_sku,
        op_kwargs={'warehouse': 'east'},
    )

    normalize_west_sku = PythonOperator(
        task_id='normalize_west_sku',
        python_callable=normalize_sku,
        op_kwargs={'warehouse': 'west'},
    )

    reconcile_inventory_task = PythonOperator(
        task_id='reconcile_inventory',
        python_callable=reconcile_inventory,
    )

    generate_reconciliation_report_task = PythonOperator(
        task_id='generate_reconciliation_report',
        python_callable=generate_reconciliation_report,
    )

    # Define task dependencies
    fetch_north_csv >> normalize_north_sku
    fetch_south_csv >> normalize_south_sku
    fetch_east_csv >> normalize_east_sku
    fetch_west_csv >> normalize_west_sku

    [normalize_north_sku, normalize_south_sku, normalize_east_sku, normalize_west_sku] >> reconcile_inventory_task
    reconcile_inventory_task >> generate_reconciliation_report_task