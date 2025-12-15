from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 300,  # 5 minutes
}

def load_sales_csv():
    """
    Load aggregated sales CSV data from the upstream DAG output.
    Validate data format and completeness.
    """
    file_path = '/path/to/aggregated_sales.csv'
    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("Sales data is empty")
    # Additional validation logic can be added here
    return df

def generate_dashboard(**context):
    """
    Generate executive dashboard with revenue trends, product performance metrics, and regional analysis visualizations.
    """
    df = context['ti'].xcom_pull(task_ids='load_sales_csv')
    # Generate dashboard logic here
    print("Dashboard generated successfully")

with DAG(
    dag_id='executive_sales_dashboard',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
) as dag:

    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id='wait_for_sales_aggregation',
        external_dag_id='daily_sales_aggregation',
        external_task_id='final_task',
        mode='reschedule',
        poke_interval=60,  # 60 seconds
        timeout=3600,  # 1 hour
    )

    load_sales_csv_task = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
    )

    generate_dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
        provide_context=True,
    )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='admin@example.com',
        subject='Executive Sales Dashboard Pipeline Failed',
        html_content='The executive sales dashboard pipeline has failed. Please check the logs for more details.',
        trigger_rule='one_failed',
    )

    wait_for_sales_aggregation >> load_sales_csv_task >> generate_dashboard_task >> send_failure_email