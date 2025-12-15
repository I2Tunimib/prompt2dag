from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import hashlib
import boto3

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to check for new data versions and download if necessary
def check_and_download_ensembl_data():
    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    ftp_base_url = 'ftp://ftp.ensembl.org/pub/current_variation/'
    s3_bucket = 'your-s3-bucket'
    s3_prefix = 'raw/landing/ensembl/'

    s3 = boto3.client('s3')

    for data_type in data_types:
        remote_file = f'{ftp_base_url}{data_type}.tsv.gz'
        local_file = f'/tmp/{data_type}.tsv.gz'
        md5_file = f'{remote_file}.md5'

        # Fetch remote MD5 checksum
        response = requests.get(md5_file)
        remote_md5 = response.text.split()[0]

        # Check if file exists in S3
        try:
            s3.head_object(Bucket=s3_bucket, Key=f'{s3_prefix}{data_type}.tsv.gz')
            s3_md5 = s3.head_object(Bucket=s3_bucket, Key=f'{s3_prefix}{data_type}.tsv.gz')['ETag'].strip('"')
        except s3.exceptions.ClientError:
            s3_md5 = None

        # Download and validate if new version is available
        if s3_md5 != remote_md5:
            response = requests.get(remote_file)
            with open(local_file, 'wb') as f:
                f.write(response.content)

            # Validate MD5 checksum
            with open(local_file, 'rb') as f:
                local_md5 = hashlib.md5(f.read()).hexdigest()

            if local_md5 == remote_md5:
                s3.upload_file(local_file, s3_bucket, f'{s3_prefix}{data_type}.tsv.gz')
            else:
                raise ValueError(f"MD5 checksum validation failed for {data_type}")

# Function to process the data using Spark
def process_ensembl_data():
    # Placeholder for Spark application logic
    # This function should be replaced with the actual Spark application
    print("Processing Ensembl data using Spark")

# DAG definition
with DAG(
    'ensembl_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to import and process Ensembl genomic mapping data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to check and download new data versions
    check_and_download_task = PythonOperator(
        task_id='check_and_download_ensembl_data',
        python_callable=check_and_download_ensembl_data,
    )

    # Task to process the data using Spark
    process_data_task = SparkKubernetesOperator(
        task_id='process_ensembl_data',
        namespace='default',
        application_file='path/to/spark-app.yaml',
        do_xcom_push=True,
    )

    # Task to send Slack notification on pipeline start
    slack_start_notification = SlackAPIPostOperator(
        task_id='slack_start_notification',
        token='your-slack-token',
        channel='#airflow-notifications',
        text='Ensembl ETL pipeline started',
    )

    # Task to send Slack notification on pipeline completion
    slack_completion_notification = SlackAPIPostOperator(
        task_id='slack_completion_notification',
        token='your-slack-token',
        channel='#airflow-notifications',
        text='Ensembl ETL pipeline completed',
    )

    # Task to send Slack notification on task failure
    slack_failure_notification = SlackAPIPostOperator(
        task_id='slack_failure_notification',
        token='your-slack-token',
        channel='#airflow-notifications',
        text='Ensembl ETL pipeline failed',
        trigger_rule='one_failed',
    )

    # Define task dependencies
    slack_start_notification >> check_and_download_task >> process_data_task >> slack_completion_notification
    [check_and_download_task, process_data_task] >> slack_failure_notification