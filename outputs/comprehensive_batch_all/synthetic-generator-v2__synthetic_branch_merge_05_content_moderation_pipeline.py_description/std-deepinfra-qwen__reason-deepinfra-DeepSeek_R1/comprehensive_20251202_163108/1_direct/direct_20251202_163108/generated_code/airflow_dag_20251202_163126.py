from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def scan_csv(**kwargs):
    """Simulate scanning a CSV file for user-generated content."""
    # Simulated metadata
    metadata = {'total_items': 100}
    kwargs['ti'].xcom_push(key='scan_results', value=metadata)
    return metadata

def toxicity_check(**kwargs):
    """Evaluate the toxicity level of the content and decide the next task."""
    # Simulated toxicity score
    toxicity_score = 0.65  # Example score
    if toxicity_score >= 0.7:
        return 'remove_and_flag_content'
    else:
        return 'publish_content'

def remove_and_flag_content(**kwargs):
    """Remove toxic content and flag the associated user account for review."""
    # Simulated action
    print("Toxic content removed and user flagged.")
    return {'action': 'remove_and_flag'}

def publish_content(**kwargs):
    """Publish safe content to the platform."""
    # Simulated action
    print("Safe content published.")
    return {'action': 'publish'}

def audit_log(**kwargs):
    """Create a final audit log entry."""
    # Retrieve results from XCom
    ti = kwargs['ti']
    scan_results = ti.xcom_pull(task_ids='scan_csv', key='scan_results')
    action_results = ti.xcom_pull(task_ids=['remove_and_flag_content', 'publish_content'], key='action')
    # Simulated audit log
    audit_entry = {
        'scan_results': scan_results,
        'action_results': action_results
    }
    print(f"Audit log created: {audit_entry}")
    return audit_entry

with DAG(
    dag_id='content_moderation_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
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
    toxicity_check_task >> [remove_and_flag_content_task, publish_content_task]
    [remove_and_flag_content_task, publish_content_task] >> audit_log_task