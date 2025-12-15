from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Mock functions to simulate real operations
def fetch_warehouse_csv(warehouse):
    """Simulate fetching CSV file from a warehouse."""
    print(f"Fetching CSV file from {warehouse} warehouse.")
    return f"{warehouse}_inventory.csv"

def normalize_sku(warehouse):
    """Simulate normalizing SKU formats for a warehouse."""
    print(f"Normalizing SKU formats for {warehouse} warehouse.")
    return f"{warehouse}_normalized_inventory.csv"

def reconcile_inventory():
    """Simulate reconciling inventory discrepancies across all warehouses."""
    print("Reconciling inventory discrepancies.")
    return "discrepancy_report.csv"

def generate_report():
    """Simulate generating the final reconciliation report."""
    print("Generating final reconciliation report.")
    return "final_reconciliation_report.pdf"

# Define the DAG
with DAG(
    dag_id='retail_inventory_reconciliation',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Fetch CSV files from warehouses
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

    # Normalize SKU formats
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

    # Reconcile inventory discrepancies
    reconcile_inventory_task = PythonOperator(
        task_id='reconcile_inventory',
        python_callable=reconcile_inventory,
    )

    # Generate final reconciliation report
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # Define task dependencies
    fetch_north_csv >> normalize_north_sku
    fetch_south_csv >> normalize_south_sku
    fetch_east_csv >> normalize_east_sku
    fetch_west_csv >> normalize_west_sku

    [normalize_north_sku, normalize_south_sku, normalize_east_sku, normalize_west_sku] >> reconcile_inventory_task
    reconcile_inventory_task >> generate_report_task