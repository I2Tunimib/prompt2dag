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

# Function to check for new data versions and download/upload to S3
def check_and_update_ensembl_data():
    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    s3_client = boto3.client('s3')
    bucket_name = 'your-s3-bucket-name'
    base_url = 'ftp://ftp.ensembl.org/pub/current_gtf/homo_sapiens/'

    for data_type in data_types:
        remote_file = f'{data_type}.tsv.gz'
        remote_md5_file = f'{data_type}.tsv.gz.md5'
        local_file = f'/tmp/{remote_file}'
        local_md5_file = f'/tmp/{remote_md5_file}'

        # Download MD5 file
        md5_response = requests.get(f'{base_url}{remote_md5_file}')
        with open(local_md5_file, 'wb') as f:
            f.write(md5_response.content)

        # Read MD5 checksum
        with open(local_md5_file, 'r') as f:
            remote_md5 = f.read().split()[0]

        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f'raw/landing/ensembl/{remote_file}')
            s3_md5 = s3_client.get_object(Bucket=bucket_name, Key=f'raw/landing/ensembl/{remote_file}')['ETag'].strip('"')
        except s3_client.exceptions.ClientError:
            s3_md5 = None

        # Download and upload if new version is found
        if s3_md5 != remote_md5:
            file_response = requests.get(f'{base_url}{remote_file}')
            with open(local_file, 'wb') as f:
                f.write(file_response.content)

            # Validate MD5 checksum
            with open(local_file, 'rb') as f:
                local_md5 = hashlib.md5(f.read()).hexdigest()

            if local_md5 == remote_md5:
                s3_client.upload_file(local_file, bucket_name, f'raw/landing/ensembl/{remote_file}')
            else:
                raise ValueError(f"MD5 checksum mismatch for {remote_file}")

# Function to process the data using Spark
def process_ensembl_data():
    # This function would contain the logic to submit a Spark job
    # For example, using the SparkSubmitOperator or SparkKubernetesOperator
    pass

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

    # Task to check and update Ensembl data
    check_and_update_task = PythonOperator(
        task_id='check_and_update_ensembl_data',
        python_callable=check_and_update_ensembl_data,
    )

    # Task to process the data using Spark
    process_data_task = SparkKubernetesOperator(
        task_id='process_ensembl_data',
        namespace='default',
        application_file='path/to/your/spark-job.yaml',
        do_xcom_push=True,
    )

    # Task to send Slack notification on pipeline start
    slack_start_notification = SlackAPIPostOperator(
        task_id='slack_start_notification',
        token='your-slack-token',
        channel='#your-channel',
        text='Ensembl ETL pipeline has started.',
    )

    # Task to send Slack notification on pipeline completion
    slack_completion_notification = SlackAPIPostOperator(
        task_id='slack_completion_notification',
        token='your-slack-token',
        channel='#your-channel',
        text='Ensembl ETL pipeline has completed.',
    )

    # Task to send Slack notification on task failure
    slack_failure_notification = SlackAPIPostOperator(
        task_id='slack_failure_notification',
        token='your-slack-token',
        channel='#your-channel',
        text='Ensembl ETL pipeline has failed.',
        trigger_rule='one_failed',
    )

    # Define task dependencies
    slack_start_notification >> check_and_update_task >> process_data_task >> slack_completion_notification
    [check_and_update_task, process_data_task] >> slack_failure_notification