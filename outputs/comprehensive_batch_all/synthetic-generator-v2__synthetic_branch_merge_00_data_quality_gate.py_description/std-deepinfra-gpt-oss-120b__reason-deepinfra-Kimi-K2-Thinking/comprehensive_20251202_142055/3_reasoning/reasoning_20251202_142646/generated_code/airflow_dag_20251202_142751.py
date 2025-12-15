from datetime import timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_csv(**context: Any) -> Dict[str, Any]:
    """Ingest raw customer CSV data and return file metadata."""
    file_metadata = {
        'file_path': '/data/customers.csv',
        'file_size': 1024000,
        'record_count': 10000,
        'ingestion_timestamp': context['ts']
    }
    context['task_instance'].xcom_push(key='file_metadata', value=file_metadata)
    return file_metadata

def quality_check(**context: Any) -> str:
    """Calculate data quality score and branch based on threshold."""
    ti = context['task_instance']
    file_metadata = ti.xcom_pull(task_ids='ingest_csv', key='file_metadata')
    
    if not file_metadata:
        raise ValueError("No file metadata found from ingest_csv task")
    
    # Simulate quality score calculation (completeness + validity metrics)
    quality_score = 97.5  # Simulated score above 95% threshold
    
    ti.xcom_push(key='quality_score', value=quality_score)
    
    # Branch based on quality threshold
    return 'production_load' if quality_score >= 95.0 else 'quarantine_and_alert'

def production_load(**context: Any) -> str:
    """Load high-quality data to production database."""
    ti = context['task_instance']
    file_metadata = ti.xcom_pull(task_ids='ingest_csv', key='file_metadata')
    quality_score = ti.xcom_pull(task_ids='quality_check', key='quality_score')
    
    print(f"Loading {file_metadata['file_path']} to production DB")
    print(f"Quality score: {quality_score}%")
    print("Production load completed successfully")
    
    return 'production_load_success'

def quarantine_and_alert(**context: Any) -> str:
    """Move low-quality data to quarantine storage."""
    ti = context['task_instance']
    file_metadata = ti.xcom_pull(task_ids='ingest_csv', key='file_metadata')
    quality_score = ti.xcom_pull(task_ids='quality_check', key='quality_score')
    
    quarantine_path = f"/quarantine/customers_{context['ts_nodash']}.csv"
    print(f"Moving {file_metadata['file_path']} to quarantine: {quarantine_path}")
    print(f"Quality score: {quality_score}% (below threshold)")
    
    ti.xcom_push(key='quarantine_path', value=quarantine_path)
    
    return 'quarantine_success'

def send_alert_email(**context: Any) -> str:
    """Send email alert to data stewards about quarantined data."""
    ti = context['task_instance']
    quality_score = ti.xcom_pull(task_ids='quality_check', key='quality_score')
    file_metadata = ti.xcom_pull(task_ids='ingest_csv', key='file_metadata')
    
    print(f"ALERT: Data quality below threshold for {file_metadata['file_path']}")
    print(f"Quality score: {quality_score}% (required: 95%)")
    print("Email sent to data stewards: data-stewards@company.com")
    
    return 'alert_sent'

def cleanup(**context: Any) -> str:
    """Perform final cleanup of temporary files and resources."""
    ti = context['task_instance']
    file_metadata = ti.xcom_pull(task_ids='ingest_csv', key='file_metadata')
    quarantine_path = ti.xcom_pull(task_ids='quarantine_and_alert', key='quarantine_path')
    
    print(f"Cleaning up temporary files for {file_metadata['file_path']}")
    
    if quarantine_path:
        print(f"Removing quarantine file: {quarantine_path}")
    
    print("Cleanup completed successfully")
    
    return 'cleanup_success'

with DAG(
    dag_id='customer_data_quality_gate',
    default_args=default_args,
    description='Data quality gate pipeline for customer CSV data with conditional routing',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'customer-data', 'branching'],
) as dag:
    
    task_ingest_csv = PythonOperator(
        task_id='ingest_csv',
        python_callable=ingest_csv,
    )
    
    task_quality_check = BranchPythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
    )
    
    task_production_load = PythonOperator(
        task_id='production_load',
        python_callable=production_load,
    )
    
    task_quarantine_and_alert = PythonOperator(
        task_id='quarantine_and_alert',
        python_callable=quarantine_and_alert,
    )
    
    task_send_alert_email = PythonOperator(
        task_id='send_alert_email',
        python_callable=send_alert_email,
    )
    
    task_cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='none_failed_min_one_success',
    )
    
    # Define task dependencies
    task_ingest_csv >> task_quality_check
    task_quality_check >> [task_production_load, task_quarantine_and_alert]
    task_quarantine_and_alert >> task_send_alert_email
    [task_production_load, task_send_alert_email] >> task_cleanup