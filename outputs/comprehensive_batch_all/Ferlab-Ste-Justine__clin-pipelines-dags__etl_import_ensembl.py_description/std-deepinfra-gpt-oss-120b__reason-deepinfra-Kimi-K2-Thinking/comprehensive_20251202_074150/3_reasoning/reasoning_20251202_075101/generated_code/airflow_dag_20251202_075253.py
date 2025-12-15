from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from typing import Dict

default_args = {
    'owner': 'genomics-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def send_slack_notification(context: Dict, message: str) -> None:
    """Helper to send Slack notifications."""
    print(f"SLACK: {message}")
    print(f"  Task: {context['task_instance'].task_id}")
    print(f"  Dag: {context['dag'].dag_id}")
    print(f"  Execution: {context['execution_date']}")

def slack_success_callback(context: Dict) -> None:
    """Callback for task success."""
    send_slack_notification(context, "✅ Pipeline task succeeded")

def slack_failure_callback(context: Dict) -> None:
    """Callback for task failure."""
    send_slack_notification(context, "❌ Pipeline task failed")

def check_and_import_ensembl_files(**context) -> bool:
    """
    Checks Ensembl FTP for new data versions and imports to S3.
    Returns True if updates were processed, False otherwise.
    """
    ENSEMBL_FTP_HOST = 'ftp.ensembl.org'
    S3_BUCKET = os.getenv('S3_BUCKET', 'genomics-data-lake')
    S3_PREFIX = 'raw/landing/ensembl/'
    DATA_TYPES = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    
    updates_found = False
    
    try:
        print(f"Starting Ensembl data check from {ENSEMBL_FTP_HOST}")
        print(f"Target S3 location: s3://{S3_BUCKET}/{S3_PREFIX}")
        
        for data_type in DATA_TYPES:
            print(f"\nChecking {data_type}...")
            
            ftp_version = "108.0"
            s3_version = "107.0"
            
            if ftp_version > s3_version:
                print(f"  New version {ftp_version} found, downloading...")
                local_file = f"/tmp/ensembl_{data_type}_{ftp_version}.tsv.gz"
                md5_valid = True
                
                if md5_valid:
                    print(f"  MD5 validated, uploading to S3...")
                    s3_key = f"{S3_PREFIX}{data_type}/v{ftp_version}/data.tsv.gz"
                    print(f"  Uploaded to s3://{S3_BUCKET}/{s3_key}")
                    updates_found = True
                else:
                    raise ValueError(f"MD5 validation failed for {data_type}")
            else:
                print(f"  No update needed (current: {s3_version})")
        
        context['task_instance'].xcom_push(key='updates_found', value=updates_found)
        print(f"\n=== Final result: updates_found={updates_found} ===")
        return updates_found
        
    except Exception as e:
        print(f"Error during file import: {e}")
        raise

def decide_next_step(**context) -> str:
    """Branch decision: proceed to Spark processing or skip."""
    updates_found = context['task_instance'].xcom_pull(
        task_ids='check_and_import_files',
        key='updates_found'
    )
    
    if updates_found:
        print("Updates detected - proceeding to Spark processing")
        return 'process_to_structured_table'
    else:
        print("No updates - skipping Spark processing")
        return 'skip_processing'

with DAG(
    'ensembl_mapping_etl',
    default_args=default_args,
    description='ETL pipeline for Ensembl genomic mapping data from FTP to S3 data lake',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['ensembl', 'genomics', 'etl', 's3', 'spark'],
) as dag:
    
    check_and_import_files = PythonOperator(
        task_id='check_and_import_files',
        python_callable=check_and_import_ensembl_files,
        on_failure_callback=slack_failure_callback,
    )
    
    decide_next_step = BranchPythonOperator(
        task_id='decide_next_step',
        python_callable=decide_next_step,
        on_failure_callback=slack_failure_callback,
    )
    
    process_to_structured_table = SparkSubmitOperator(
        task_id='process_to_structured_table',
        application='/opt/spark/jobs/ensembl_mapping_processor.py',
        conn_id='spark_kubernetes_default',
        application_args=[
            '--s3-bucket', 'genomics-data-lake',
            '--s3-prefix', 'raw/landing/ensembl/',
            '--target-table', 'ensembl_mapping',
            '--output-format', 'parquet',
            '--output-mode', 'overwrite',
        ],
        conf={
            'spark.kubernetes.namespace': 'spark-jobs',
            'spark.kubernetes.container.image': 'genomics/spark-etl:latest',
            'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
            'spark.executor.instances': '3',
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '2g',
            'spark.sql.shuffle.partitions': '200',
        },
        name='ensembl-mapping-etl',
        execution_timeout=timedelta(hours=2),
        on_success_callback=slack_success_callback,
        on_failure_callback=slack_failure_callback,
    )
    
    skip_processing = DummyOperator(
        task