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
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: SlackAPIPostOperator(
        task_id='slack_notification_on_failure',
        token='YOUR_SLACK_BOT_TOKEN',
        channel='#airflow-notifications',
        text=f"Task failed: {context['task_instance'].task_id}",
    ).execute(context),
}

# Function to check for new data versions and download/upload to S3
def check_and_update_ensembl_data():
    data_types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
    s3_client = boto3.client('s3')
    bucket_name = 'your-s3-bucket'
    ftp_base_url = 'ftp://ftp.ensembl.org/pub/current_mapping/homo_sapiens/'

    for data_type in data_types:
        remote_file = f'{data_type}.tsv'
        remote_md5_file = f'{data_type}.tsv.md5'
        local_file = f'/tmp/{remote_file}'
        local_md5_file = f'/tmp/{remote_md5_file}'
        s3_key = f'raw/landing/ensembl/{remote_file}'

        # Download MD5 file
        md5_url = f'{ftp_base_url}{remote_md5_file}'
        response = requests.get(md5_url)
        with open(local_md5_file, 'wb') as f:
            f.write(response.content)

        # Read MD5 checksum
        with open(local_md5_file, 'r') as f:
            expected_md5 = f.read().split()[0]

        # Check if file already exists in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            # File exists, check MD5
            s3_md5 = s3_client.get_object(Bucket=bucket_name, Key=s3_key)['ETag'].strip('"')
            if s3_md5 == expected_md5:
                print(f'No update needed for {data_type}')
                continue
        except s3_client.exceptions.ClientError:
            pass  # File does not exist in S3

        # Download new file
        file_url = f'{ftp_base_url}{remote_file}'
        response = requests.get(file_url)
        with open(local_file, 'wb') as f:
            f.write(response.content)

        # Validate MD5 checksum
        with open(local_file, 'rb') as f:
            actual_md5 = hashlib.md5(f.read()).hexdigest()
        if actual_md5 != expected_md5:
            raise ValueError(f'MD5 checksum mismatch for {data_type}')

        # Upload to S3
        s3_client.upload_file(local_file, bucket_name, s3_key, ExtraArgs={'Metadata': {'md5': expected_md5}})
        print(f'Updated {data_type} to S3')

# Function to process the data using Spark
def process_ensembl_data():
    # This function would contain the logic to submit a Spark job
    # For example, using the SparkSubmitOperator or SparkKubernetesOperator
    # Here we use SparkKubernetesOperator as an example
    pass

# DAG definition
with DAG(
    dag_id='ensembl_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
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

    # Task to send a Slack notification on pipeline completion
    slack_notification_task = SlackAPIPostOperator(
        task_id='slack_notification_on_completion',
        token='YOUR_SLACK_BOT_TOKEN',
        channel='#airflow-notifications',
        text='Ensembl ETL pipeline completed successfully',
    )

    # Define task dependencies
    check_and_update_task >> process_data_task >> slack_notification_task