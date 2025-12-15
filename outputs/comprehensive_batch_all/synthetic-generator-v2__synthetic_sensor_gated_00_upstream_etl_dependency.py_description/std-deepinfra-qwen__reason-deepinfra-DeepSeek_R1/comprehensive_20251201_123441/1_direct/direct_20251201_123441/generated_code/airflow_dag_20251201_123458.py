from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': 300,  # 5 minutes
    'email_on_failure': True,
    'email': ['admin@example.com']
}

def load_sales_csv():
    """Loads aggregated sales CSV data, validates format and completeness."""
    # Example CSV path, adjust as needed
    csv_path = '/path/to/aggregated_sales_data.csv'
    df = pd.read_csv(csv_path)
    # Perform data validation
    if df.empty:
        raise ValueError("Sales data is empty")
    # Additional validation logic can be added here
    return df

def generate_dashboard():
    """Generates executive dashboard with key sales metrics and visualizations."""
    # Example dashboard generation logic
    # This could involve creating charts, tables, and other visualizations
    # For simplicity, we'll just print a message
    print("Executive dashboard generated with revenue trends, product performance, and regional analysis.")

with DAG(
    dag_id='executive_sales_dashboard',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id='wait_for_sales_aggregation',
        external_dag_id='daily_sales_aggregation',
        external_task_id='final_task',  # Adjust to the final task ID of the upstream DAG
        mode='reschedule',
        poke_interval=60,  # 60 seconds
        timeout=3600,  # 1 hour
        dag=dag
    )

    load_sales_csv_task = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
        dag=dag
    )

    generate_dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
        dag=dag
    )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='admin@example.com',
        subject='Pipeline Failure',
        html_content='The executive sales dashboard pipeline has failed.',
        trigger_rule='all_failed',
        dag=dag
    )

    wait_for_sales_aggregation >> load_sales_csv_task >> generate_dashboard_task >> send_failure_email