from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 60,
    'concurrency': 1,
    'max_active_runs': 1
}

def params_validate(**kwargs):
    """Validate the color parameter provided to the pipeline."""
    color = kwargs.get('params', {}).get('color', 'default_color')
    if color not in ['red', 'blue', 'green']:
        raise ValueError("Invalid color parameter. Allowed values: red, blue, green")
    return color

def download_mondo_terms(**kwargs):
    """Download the latest Mondo OBO file from GitHub releases and upload to S3 if new."""
    s3 = boto3.client('s3')
    bucket_name = 'mondo-ontology-bucket'
    file_key = 'mondo.obo'
    # Check if a newer version exists
    # Download and upload to S3 if new
    s3.upload_file('/path/to/local/mondo.obo', bucket_name, file_key)
    return file_key

def normalized_mondo_terms(**kwargs):
    """Process the Mondo OBO file using Spark to normalize and transform the ontology terms."""
    # Spark job to normalize and transform the ontology terms
    return 'normalized_data_path'

def index_mondo_terms(**kwargs):
    """Index the normalized Mondo terms to Elasticsearch using a Spark job."""
    # Spark job to index the normalized data to Elasticsearch
    return 'indexed_data_path'

def publish_mondo(**kwargs):
    """Publish the indexed Mondo data to make it available for consumption."""
    # Publish the indexed data
    return 'published_data_path'

def send_slack_notification(**kwargs):
    """Send a Slack notification indicating pipeline completion."""
    message = "Mondo ETL pipeline completed successfully."
    return message

with DAG(
    dag_id='mondo_ontology_pipeline',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'mondo', 'ontology']
) as dag:

    validate_params = PythonOperator(
        task_id='params_validate',
        python_callable=params_validate,
        provide_context=True
    )

    check_s3_key = S3KeySensor(
        task_id='check_s3_key',
        bucket_key='mondo.obo',
        wildcard_match=True,
        bucket_name='mondo-ontology-bucket',
        aws_conn_id='aws_default',
        timeout=30,
        poke_interval=5
    )

    download_mondo = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms,
        provide_context=True
    )

    normalize_mondo = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalized_mondo_terms,
        provide_context=True
    )

    index_mondo = SparkSubmitOperator(
        task_id='index_mondo_terms',
        application='/path/to/index_mondo_terms.py',
        conn_id='spark_default',
        conf={'spark.executor.memory': '2g'},
        application_args=['--input', 'normalized_data_path', '--output', 'indexed_data_path']
    )

    publish_mondo_data = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        provide_context=True
    )

    send_slack = SlackAPIPostOperator(
        task_id='slack_notification',
        token='your_slack_token',
        channel='#airflow-notifications',
        text='{{ ti.xcom_pull(task_ids="send_slack_notification") }}',
        username='Airflow'
    )

    validate_params >> check_s3_key >> download_mondo >> normalize_mondo >> index_mondo >> publish_mondo_data >> send_slack