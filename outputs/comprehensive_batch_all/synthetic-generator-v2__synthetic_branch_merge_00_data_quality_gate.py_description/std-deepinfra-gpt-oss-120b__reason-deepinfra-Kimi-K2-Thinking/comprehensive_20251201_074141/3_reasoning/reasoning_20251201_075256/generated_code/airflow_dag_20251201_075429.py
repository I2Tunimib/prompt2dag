from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator


default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def ingest_csv(**context):
    """Ingest raw customer CSV data and return file metadata."""
    return {
        'file_path': '/data/customers.csv',
        'row_count': 1000,
        'file_size_mb': 15.7
    }


def quality_check(**context):
    """Calculate data quality score and determine routing path."""
    ti = context['task_instance']
    file_metadata = ti.xcom_pull(task_ids='ingest_csv')
    
    if file_metadata and file_metadata.get('row_count', 0) > 100:
        quality_score = 96.5
    else:
        quality_score = 85.0
    
    ti.xcom_push(key='quality_score', value=quality_score)
    
    return 'production_load' if quality_score >= 95.0 else 'quarantine_and_alert'


def production_load(**context):
    """Load high-quality data to production database."""
    print("Loading data to production database")


def quarantine_and_alert(**context):
    """Move low-quality data to quarantine storage."""
    print("Moving data to quarantine storage")


def cleanup(**context):
    """Perform final cleanup of temporary files and resources."""
    print("Cleaning up temporary files and resources")


with DAG(
    'customer_data_quality_gate',
    default_args=default_args,
    description='Data quality gate pipeline for customer CSV data with conditional routing',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['data-quality', 'customer-data'],
) as dag:
    
    ingest_csv_task = PythonOperator(
        task_id='ingest_csv',
        python_callable=ingest_csv,
    )
    
    quality_check_task = BranchPythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
    )
    
    production_load_task = PythonOperator(
        task_id='production_load',
        python_callable=production_load,
    )
    
    quarantine_and_alert_task = PythonOperator(
        task_id='quarantine_and_alert',
        python_callable=quarantine_and_alert,
    )
    
    send_alert_email_task = EmailOperator(
        task_id='send_alert_email',
        to=['data-stewards@example.com'],
        subject='Data Quality Alert: Customer Data Below Threshold',
        html_content='<p>Customer data quality score was below 95% threshold. Data has been quarantined for review.</p>',
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='none_failed',
    )
    
    ingest_csv_task >> quality_check_task
    quality_check_task >> production_load_task >> cleanup_task
    quality_check_task >> quarantine_and_alert_task >> send_alert_email_task >> cleanup_task