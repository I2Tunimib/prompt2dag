from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def scan_csv(**kwargs):
    """Scan a CSV file for user-generated content and return metadata."""
    df = pd.read_csv('/path/to/user_content.csv')
    total_items = len(df)
    kwargs['ti'].xcom_push(key='total_items', value=total_items)
    return total_items

def toxicity_check(**kwargs):
    """Evaluate the toxicity level of the content and route to the appropriate task."""
    ti = kwargs['ti']
    total_items = ti.xcom_pull(task_ids='scan_csv', key='total_items')
    # Mock toxicity score for demonstration
    toxicity_score = 0.8 if total_items > 100 else 0.5
    ti.xcom_push(key='toxicity_score', value=toxicity_score)
    if toxicity_score >= 0.7:
        return 'remove_and_flag_content'
    else:
        return 'publish_content'

def remove_and_flag_content(**kwargs):
    """Remove toxic content and flag the associated user account for review."""
    ti = kwargs['ti']
    toxicity_score = ti.xcom_pull(task_ids='toxicity_check', key='toxicity_score')
    print(f"Removing toxic content with score: {toxicity_score}")
    # Logic to remove and flag content
    ti.xcom_push(key='action', value='removed')

def publish_content(**kwargs):
    """Publish safe content to the platform."""
    ti = kwargs['ti']
    toxicity_score = ti.xcom_pull(task_ids='toxicity_check', key='toxicity_score')
    print(f"Publishing safe content with score: {toxicity_score}")
    # Logic to publish content
    ti.xcom_push(key='action', value='published')

def audit_log(**kwargs):
    """Create a final audit log entry."""
    ti = kwargs['ti']
    action = ti.xcom_pull(task_ids=['remove_and_flag_content', 'publish_content'], key='action')
    print(f"Creating audit log for action: {action}")
    # Logic to create audit log
    ti.xcom_push(key='audit_log', value=f"Audit log created for action: {action}")

with DAG(
    dag_id='content_moderation_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    scan_csv_task = PythonOperator(
        task_id='scan_csv',
        python_callable=scan_csv,
        provide_context=True,
    )

    toxicity_check_task = BranchPythonOperator(
        task_id='toxicity_check',
        python_callable=toxicity_check,
        provide_context=True,
    )

    remove_and_flag_content_task = PythonOperator(
        task_id='remove_and_flag_content',
        python_callable=remove_and_flag_content,
        provide_context=True,
    )

    publish_content_task = PythonOperator(
        task_id='publish_content',
        python_callable=publish_content,
        provide_context=True,
    )

    audit_log_task = PythonOperator(
        task_id='audit_log',
        python_callable=audit_log,
        provide_context=True,
    )

    scan_csv_task >> toxicity_check_task
    toxicity_check_task >> remove_and_flag_content_task
    toxicity_check_task >> publish_content_task
    remove_and_flag_content_task >> audit_log_task
    publish_content_task >> audit_log_task