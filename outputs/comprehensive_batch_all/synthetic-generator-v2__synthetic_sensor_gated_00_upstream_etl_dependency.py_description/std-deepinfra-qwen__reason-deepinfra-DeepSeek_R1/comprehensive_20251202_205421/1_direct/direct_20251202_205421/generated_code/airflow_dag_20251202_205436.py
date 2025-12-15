from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def load_sales_csv():
    """Loads and validates the aggregated sales CSV data."""
    # Example path to the CSV file
    file_path = '/path/to/aggregated_sales_data.csv'
    try:
        df = pd.read_csv(file_path)
        # Perform data validation
        if df.empty:
            raise ValueError("CSV file is empty")
        # Additional validation logic can be added here
        print("Sales data loaded and validated successfully")
    except Exception as e:
        raise ValueError(f"Failed to load or validate sales data: {e}")

def generate_dashboard():
    """Generates the executive dashboard with key sales metrics and visualizations."""
    # Example path to the CSV file
    file_path = '/path/to/aggregated_sales_data.csv'
    try:
        df = pd.read_csv(file_path)
        # Generate dashboard logic
        # Example: Generate revenue trends, product performance metrics, and regional analysis
        print("Executive dashboard generated successfully")
    except Exception as e:
        raise ValueError(f"Failed to generate dashboard: {e}")

with DAG(
    dag_id='executive_sales_dashboard',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id='wait_for_sales_aggregation',
        external_dag_id='daily_sales_aggregation',
        external_task_id='final_task',  # Replace with the actual final task ID of the upstream DAG
        mode='reschedule',
        poke_interval=60,  # Poll every 60 seconds
        timeout=3600,  # Timeout after 1 hour
    )

    load_sales_csv_task = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
    )

    generate_dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
    )

    wait_for_sales_aggregation >> load_sales_csv_task >> generate_dashboard_task