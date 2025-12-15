import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_warehouse_data(warehouse_name, **kwargs):
    """Simulate fetching CSV file from a warehouse."""
    file_path = f"/tmp/{warehouse_name}_inventory_{kwargs['ds']}.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        f.write(f"sku,quantity\n{warehouse_name}_SKU001,100\n")
    return file_path


def normalize_sku_data(warehouse_name, **kwargs):
    """Simulate SKU normalization for a warehouse file."""
    ti = kwargs['ti']
    fetch_task_id = f'fetch_{warehouse_name}'
    file_path = ti.xcom_pull(task_ids=fetch_task_id)
    
    normalized_path = f"/tmp/normalized_{warehouse_name}_inventory_{kwargs['ds']}.csv"
    os.makedirs(os.path.dirname(normalized_path), exist_ok=True)
    with open(normalized_path, 'w') as f:
        f.write(f"standard_sku,quantity\nSTD_{warehouse_name}_SKU001,100\n")
    
    return normalized_path


def reconcile_inventory(**kwargs):
    """Reconcile inventory across all warehouses and generate discrepancy report."""
    ti = kwargs['ti']
    warehouses = ['north', 'south', 'east', 'west']
    
    normalized_files = {}
    for warehouse in warehouses:
        task_id = f'normalize_{warehouse}'
        path = ti.xcom_pull(task_ids=task_id)
        normalized_files[warehouse] = path
    
    discrepancy_report_path = f"/tmp/discrepancy_report_{kwargs['ds']}.csv"
    os.makedirs(os.path.dirname(discrepancy_report_path), exist_ok=True)
    
    with open(discrepancy_report_path, 'w') as f:
        f.write("sku,north_qty,south_qty,east_qty,west_qty,discrepancy\n")
        for i in range(23):
            f.write(f"DISCREPANCY_SKU{i:03d},100,95,100,90,True\n")
    
    return discrepancy_report_path


def generate_reconciliation_report(**kwargs):
    """Generate final PDF reconciliation report from discrepancy data."""
    ti = kwargs['ti']
    discrepancy_report_path = ti.xcom_pull(task_ids='reconcile_inventory')
    
    pdf_path = f"/tmp/reconciliation_report_{kwargs['ds']}.pdf"
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    
    with open(pdf_path, 'w') as f:
        f.write(f"PDF Report Generated from {discrepancy_report_path}\n")
        f.write("SKU Discrepancies, Quantity Variances, Reconciliation Recommendations\n")
    
    return pdf_path


default_args = {
    'owner': 'retail-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_inventory_reconciliation',
    default_args=default_args,
    description='Daily retail inventory reconciliation across four warehouses',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'inventory', 'reconciliation'],
) as dag:
    
    fetch_north = PythonOperator(
        task_id='fetch_north',
        python_callable=fetch_warehouse_data,
        op_kwargs={'warehouse_name': 'north'},
    )
    
    fetch_south = PythonOperator(
        task_id='fetch_south',
        python_callable=fetch_warehouse_data,
        op_kwargs={'warehouse_name': 'south'},
    )
    
    fetch_east = PythonOperator(
        task_id='fetch_east',
        python_callable=fetch_warehouse_data,
        op_kwargs={'warehouse_name': 'east'},
    )
    
    fetch_west = PythonOperator(
        task_id='fetch_west',
        python_callable=fetch_warehouse_data,
        op_kwargs={'warehouse_name': 'west'},
    )
    
    normalize_north = PythonOperator(
        task_id='normalize_north',
        python_callable=normalize_sku_data,
        op_kwargs={'warehouse_name': 'north'},
    )
    
    normalize_south = PythonOperator(
        task_id='normalize_south',
        python_callable=normalize_sku_data,
        op_kwargs={'warehouse_name': 'south'},
    )
    
    normalize_east = PythonOperator(
        task_id='normalize_east',
        python_callable=normalize_sku_data,
        op_kwargs={'warehouse_name': 'east'},
    )
    
    normalize_west = PythonOperator(
        task_id='normalize_west',
        python_callable=normalize_sku_data,
        op_kwargs={'warehouse_name': 'west'},
    )
    
    reconcile_inventory_task = PythonOperator(
        task_id='reconcile_inventory',
        python_callable=reconcile_inventory,
    )
    
    generate_report = PythonOperator(
        task_id='generate_reconciliation_report',
        python_callable=generate_reconciliation_report,
    )
    
    # Define task dependencies
    fetch_north >> normalize_north
    fetch_south >> normalize_south
    fetch_east >> normalize_east
    fetch_west >> normalize_west
    
    [normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile_inventory_task
    
    reconcile_inventory_task >> generate_report