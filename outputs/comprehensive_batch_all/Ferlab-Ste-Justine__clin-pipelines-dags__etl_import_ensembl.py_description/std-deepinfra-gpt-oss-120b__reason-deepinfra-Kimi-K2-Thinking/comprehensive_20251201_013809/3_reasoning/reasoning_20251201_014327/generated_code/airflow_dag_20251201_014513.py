from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
import boto3
from ftplib import FTP
import hashlib

# For Slack notifications
from airflow.providers.slack.notifications.slack import send_slack_notification

# Default args with Slack failure callback
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': [send_slack_notification(
        text="Task failed: {{ dag.dag_id }} - {{ task.task_id }}",
        channel="#data-pipelines"
    )],
}

# Slack success callback for pipeline completion
def slack_success_callback(context):
    send_slack_notification(
        text="Pipeline completed successfully: {{ dag.dag_id }}",
        channel="#data-pipelines"
    )

# Python function for file task
def check_and_import_ensembl_files(**context):
    """
    Check Ensembl FTP for new data versions and import to S3 if updated.
    """
    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    s3_bucket = 'your-s3-bucket'  # Should come from Variable or Connection
    s3_prefix = 'raw/landing/ensembl/'
    
    # Get S3 connection
    s3_conn = BaseHook.get_connection('aws_default')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        region_name='us-east-1'
    )
    
    # Get FTP connection details
    ftp_host = 'ftp.ensembl.org'
    ftp_path = '/pub/current_tsv/homo_sapiens/'
    
    updates_found = False
    
    try:
        ftp = FTP(ftp_host)
        ftp.login()  # Anonymous login
        
        for data_type in data_types:
            # Construct filename pattern (this is illustrative)
            filename = f"{data_type}_mapping.tsv.gz"
            md5_filename = f"{filename}.md5"
            
            # Check current version in S3
            s3_key = f"{s3_prefix}{filename}"
            try:
                s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
                s3_exists = True
                # Get S3 file metadata for version comparison
                # In real scenario, you'd store version metadata separately
            except:
                s3_exists = False
            
            # Check FTP for file
            try:
                ftp.cwd(ftp_path)
                files = ftp.nlst()
                
                if filename in files:
                    # Download file and MD5
                    local_path = f"/tmp/{filename}"
                    md5_local_path = f"/tmp/{md5_filename}"
                    
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {filename}', f.write)
                    
                    with open(md5_local_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {md5_filename}', f.write)
                    
                    # Validate MD5
                    with open(md5_local_path, 'r') as f:
                        expected_md5 = f.read().split()[0]
                    
                    with open(local_path, 'rb') as f:
                        actual_md5 = hashlib.md5(f.read()).hexdigest()
                    
                    if actual_md5 == expected_md5:
                        # Upload to S3
                        s3_client.upload_file(local_path, s3_bucket, s3_key)
                        updates_found = True
                        
                        # Cleanup
                        os.remove(local_path)
                        os.remove(md5_local_path)
                    else:
                        raise Exception(f"MD5 mismatch for {filename}")
                else:
                    print(f"File {filename} not found on FTP")
                    
            except Exception as e:
                print(f"Error processing {data_type}: {e}")
                continue
        
        ftp.quit()
        
    except Exception as e:
        raise Exception(f"FTP connection failed: {e}")
    
    # If no updates found, skip downstream tasks
    if not updates_found:
        raise AirflowSkipException("No new Ensembl data versions found. Skipping downstream tasks.")

# DAG definition
with DAG(
    dag_id='ensembl_mapping_etl',
    default_args=default_args,
    description='ETL pipeline for Ensembl genomic mapping data to S3 and Spark processing',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['ensembl', 'genomics', 'etl'],
    on_success_callback=slack_success_callback,
) as dag:
    
    # Task 1: Check and import files
    check_and_import_files = PythonOperator(
        task_id='check_and_import_files',
        python_callable=check_and_import_ensembl_files,
    )
    
    # Task 2: Process with Spark
    process_with_spark = SparkSubmitOperator(
        task_id='process_with_spark',
        application='/path/to/spark/ensembl_processing.py',  # Path to Spark script
        conn_id='spark_k8s_default',  # Spark connection ID
        conf={
            'spark.kubernetes.container.image': 'your-spark-image:latest',
            'spark.kubernetes.namespace': 'spark-jobs',
            'spark.executor.instances': '3',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
        },
        application_args=[
            '--s3-bucket', 'your-s3-bucket',
            '--s3-prefix', 'raw/landing/ensembl/',
            '--output-table', 'ensembl_mapping',
        ],
        env_vars={
            'AWS_ACCESS_KEY_ID': '{{ conn.aws_default.login }}',
            'AWS_SECRET_ACCESS_KEY': '{{ conn.aws_default.password }}',
        },
    )
    
    # Task dependencies
    check_and_import_files >> process_with_spark