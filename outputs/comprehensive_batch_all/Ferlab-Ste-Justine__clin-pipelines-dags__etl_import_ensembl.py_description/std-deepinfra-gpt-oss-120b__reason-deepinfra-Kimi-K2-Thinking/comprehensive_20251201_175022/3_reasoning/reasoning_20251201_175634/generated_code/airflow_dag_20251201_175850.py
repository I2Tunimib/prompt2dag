from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.slack.notifications.slack import send_slack_notification

# Set up logging
logger = logging.getLogger(__name__)

# Default args
default_args = {
    'owner': 'genomics-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'ensembl_genomic_mapping_etl',
    default_args=default_args,
    description='ETL pipeline for Ensembl genomic mapping data',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['genomics', 'ensembl', 'etl'],
    on_success_callback=send_slack_notification(
        slack_conn_id='slack_default',
        text="Pipeline ensembl_genomic_mapping_etl completed successfully",
        channel="#genomics-alerts"
    ),
    on_failure_callback=send_slack_notification(
        slack_conn_id='slack_default',
        text="Pipeline ensembl_genomic_mapping_etl failed",
        channel="#genomics-alerts"
    )
) as dag:
    
    # Task 1: Check for new data and import to S3
    def check_and_import_ensembl_data(**context):
        """
        Checks Ensembl FTP for new data versions and imports to S3 if updates found.
        Raises AirflowSkipException if no updates are available.
        """
        data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
        updates_found = False
        
        try:
            # Import required libraries
            import ftplib
            import boto3
            import hashlib
            import os
            
            # Configuration - in production, these should come from Airflow Variables or Connections
            ftp_host = 'ftp.ensembl.org'
            ftp_path = '/pub/current_tsv/homo_sapiens/'
            s3_bucket = 'genomics-data-lake'
            s3_prefix = 'raw/landing/ensembl/'
            
            # Connect to FTP and S3
            ftp = ftplib.FTP(ftp_host)
            ftp.login()
            s3_client = boto3.client('s3')
            
            for data_type in data_types:
                # Check current version in S3
                version_key = f"{s3_prefix}{data_type}_version.txt"
                current_version = None
                
                try:
                    response = s3_client.get_object(Bucket=s3_bucket, Key=version_key)
                    current_version = response['Body'].read().decode('utf-8').strip()
                except s3_client.exceptions.NoSuchKey:
                    logger.info(f"No existing version found for {data_type}, will download")
                    current_version = None
                
                # Get latest version from FTP
                # This is simplified - actual implementation would parse FTP directory listings
                # For this example, we'll assume version is in a specific file
                version_file = f"{data_type}_version.txt"
                
                try:
                    ftp.cwd(ftp_path)
                    # Download version file to temp location
                    temp_version_path = f"/tmp/{version_file}"
                    with open(temp_version_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {version_file}', f.write)
                    
                    with open(temp_version_path, 'r') as f:
                        latest_version = f.read().strip()
                    
                    os.remove(temp_version_path)
                except ftplib.error_perm:
                    logger.warning(f"Version file not found for {data_type}, skipping")
                    continue
                
                # Compare versions
                if current_version != latest_version:
                    logger.info(f"New version found for {data_type}: {latest_version}")
                    updates_found = True
                    
                    # Download the actual data file
                    data_file = f"{data_type}_mapping.tsv"
                    temp_data_path = f"/tmp/{data_file}"
                    
                    with open(temp_data_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {data_file}', f.write)
                    
                    # Download MD5 checksum
                    md5_file = f"{data_file}.md5"
                    temp_md5_path = f"/tmp/{md5_file}"
                    
                    try:
                        with open(temp_md5_path, 'wb') as f:
                            ftp.retrbinary(f'RETR {md5_file}', f.write)
                        
                        # Validate MD5
                        with open(temp_md5_path, 'r') as f:
                            expected_md5 = f.read().split()[0]
                        
                        # Calculate actual MD5
                        hash_md5 = hashlib.md5()
                        with open(temp_data_path, 'rb') as f:
                            for chunk in iter(lambda: f.read(4096), b""):
                                hash_md5.update(chunk)
                        actual_md5 = hash_md5.hexdigest()
                        
                        if actual_md5 != expected_md5:
                            raise ValueError(f"MD5 checksum mismatch for {data_file}")
                        
                        os.remove(temp_md5_path)
                    except ftplib.error_perm:
                        logger.warning(f"No MD5 file found for {data_file}, skipping validation")
                    
                    # Upload to S3
                    s3_data_key = f"{s3_prefix}{data_file}"
                    s3_client.upload_file(temp_data_path, s3_bucket, s3_data_key)
                    
                    # Upload version file to S3
                    with open(temp_version_path, 'w') as f:
                        f.write(latest_version)
                    s3_client.upload_file(temp_version_path, s3_bucket, version_key)
                    
                    # Cleanup
                    os.remove(temp_data_path)
                    os.remove(temp_version_path)
                    
                    logger.info(f"Successfully uploaded {data_type} data to S3")
                else:
                    logger.info(f"No updates for {data_type}, current version: {current_version}")
            
            ftp.quit()
            
            if not updates_found:
                logger.info("No updates found for any data type, skipping downstream tasks")
                raise AirflowSkipException("No new Ensembl data versions available")
            
            return "Updates found and imported successfully"
            
        except Exception as e:
            logger.error(f"Error in check_and_import_ensembl_data: {str(e)}")
            raise
    
    # Task 2: Process data with Spark
    def build_spark_task():
        # Spark configuration
        spark_app_name = "EnsemblMappingETL"
        spark_config = {
            'spark.kubernetes.namespace': 'spark-jobs',
            'spark.kubernetes.container.image': 'spark-etl:latest',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        }
        
        # The Spark script path - in production, this would be a packaged application
        # For this example, we'll assume it's a Python script
        spark_application = "s3a://genomics-data-lake/scripts/ensembl_mapping_etl.py"
        
        return SparkSubmitOperator(
            task_id='process_ensembl_data',
            application=spark_application,
            name='ensembl-mapping-etl',
            conf=spark_config,
            application_args=[
                '--source-path', 's3a://genomics-data-lake/raw/landing/ensembl/',
                '--target-table', 'ensembl_mapping',
                '--data-types', 'canonical,ena,entrez,refseq,uniprot'
            ],
            env_vars={
                'AWS_REGION': 'us-east-1',
            },
            retries=2,
            retry_delay=timedelta(minutes=10),
            on_failure_callback=send_slack_notification(
                slack_conn_id='slack_default',
                text="Task process_ensembl_data failed in ensembl_genomic_mapping_etl pipeline",
                channel="#genomics-alerts"
            )
        )
    
    # Define tasks
    check_and_import_task = PythonOperator(
        task_id='check_and_import_ensembl_data',
        python_callable=check_and_import_ensembl_data,
        on_failure_callback=send_slack_notification(
            slack_conn_id='slack_default',
            text="Task check_and_import_ensembl_data failed in ensembl_genomic_mapping_etl pipeline",
            channel="#genomics-alerts"
        )
    )
    
    process_ensembl_task = build_spark_task()
    
    # Set dependencies
    check_and_import_task >> process_ensembl_task