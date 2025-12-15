from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sensors import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import requests
import boto3
from pyspark.sql import SparkSession

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
        raise ValueError("Invalid color parameter. Choose from 'red', 'blue', or 'green'.")

def download_mondo_terms(**kwargs):
    """Download the latest Mondo OBO file from GitHub releases and upload to S3 if new."""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_bucket = 'mondo-ontology-bucket'
    s3_key = 'mondo.obo'
    
    response = requests.get('https://api.github.com/repos/monarch-initiative/mondo/releases/latest')
    response.raise_for_status()
    latest_version = response.json()['tag_name']
    
    if not s3_hook.check_for_key(s3_key, s3_bucket):
        s3_hook.load_file(
            filename=f'https://github.com/monarch-initiative/mondo/releases/download/{latest_version}/mondo.obo',
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
    else:
        current_version = s3_hook.read_key(s3_key, s3_bucket).split('\n')[0].strip()
        if current_version != latest_version:
            s3_hook.load_file(
                filename=f'https://github.com/monarch-initiative/mondo/releases/download/{latest_version}/mondo.obo',
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True
            )

def normalized_mondo_terms(**kwargs):
    """Process the Mondo OBO file using Spark to normalize and transform the ontology terms."""
    spark = SparkSession.builder.appName("MondoNormalization").getOrCreate()
    s3_bucket = 'mondo-ontology-bucket'
    s3_key = 'mondo.obo'
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    obo_file = s3_hook.download_file(s3_key, s3_bucket)
    
    # Load and process the OBO file using Spark
    df = spark.read.text(obo_file)
    # Example transformation (this is a placeholder, actual transformation logic should be implemented)
    normalized_df = df.withColumn('normalized_term', df['value'])
    normalized_df.write.parquet('s3a://' + s3_bucket + '/normalized_mondo_terms', mode='overwrite')

def index_mondo_terms(**kwargs):
    """Index the normalized Mondo terms to Elasticsearch using a Spark job."""
    spark = SparkSession.builder.appName("MondoIndexing").getOrCreate()
    s3_bucket = 'mondo-ontology-bucket'
    
    # Load the normalized data
    normalized_df = spark.read.parquet('s3a://' + s3_bucket + '/normalized_mondo_terms')
    
    # Index to Elasticsearch (this is a placeholder, actual indexing logic should be implemented)
    normalized_df.write.format('org.elasticsearch.spark.sql').option('es.resource', 'mondo/terms').save()

def publish_mondo(**kwargs):
    """Publish the indexed Mondo data to make it available for consumption."""
    # Placeholder for publishing logic
    print("Publishing indexed Mondo data...")

def slack_notification(**kwargs):
    """Send a Slack notification indicating pipeline completion."""
    SlackAPIPostOperator(
        task_id='slack_notification',
        token='your_slack_token',
        channel='#airflow-notifications',
        text='Mondo ETL pipeline completed successfully!',
        dag=dag
    ).execute(context=kwargs)

with DAG(
    dag_id='mondo_ontology_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'mondo', 'ontology']
) as dag:

    validate_params = PythonOperator(
        task_id='params_validate',
        python_callable=params_validate,
        provide_context=True
    )

    download_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms,
        provide_context=True
    )

    normalize_terms = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalized_mondo_terms,
        provide_context=True
    )

    index_terms = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo_terms,
        provide_context=True
    )

    publish_data = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        provide_context=True
    )

    notify_completion = PythonOperator(
        task_id='slack_notification',
        python_callable=slack_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    validate_params >> download_terms >> normalize_terms >> index_terms >> publish_data >> notify_completion