from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import boto3
import requests
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': 300,
    'concurrency': 1,
    'max_active_runs': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'trigger_rule': TriggerRule.NONE_FAILED
}

def params_validate(**kwargs):
    color = kwargs.get('params', {}).get('color', 'default_color')
    if color not in ['red', 'blue', 'green']:
        raise ValueError("Invalid color parameter")
    print(f"Validated color: {color}")

def download_mondo_terms(**kwargs):
    url = "https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.obo"
    response = requests.get(url)
    if response.status_code == 200:
        s3 = boto3.client('s3')
        bucket_name = 'your-s3-bucket'
        key = 'mondo/mondo.obo'
        s3.put_object(Body=response.content, Bucket=bucket_name, Key=key)
        print(f"Downloaded and uploaded Mondo OBO file to S3: {bucket_name}/{key}")
    else:
        raise Exception("Failed to download Mondo OBO file")

def normalized_mondo_terms(**kwargs):
    spark = SparkSession.builder.appName("MondoNormalization").getOrCreate()
    s3_path = "s3a://your-s3-bucket/mondo/mondo.obo"
    df = spark.read.text(s3_path)
    # Process and normalize the data
    normalized_df = df.withColumn("normalized_column", df["value"])
    normalized_df.write.parquet("s3a://your-s3-bucket/mondo/normalized")
    print("Normalized Mondo terms and stored in data lake")

def index_mondo_terms(**kwargs):
    spark = SparkSession.builder.appName("MondoIndexing").getOrCreate()
    normalized_path = "s3a://your-s3-bucket/mondo/normalized"
    df = spark.read.parquet(normalized_path)
    # Index the data to Elasticsearch
    df.write.format("org.elasticsearch.spark.sql").option("es.resource", "mondo/terms").save()
    print("Indexed Mondo terms to Elasticsearch")

def publish_mondo(**kwargs):
    # Logic to publish indexed data
    print("Published Mondo data for consumption")

def slack_notification(**kwargs):
    message = "Mondo ETL pipeline completed successfully"
    SlackAPIPostOperator(
        task_id='slack_notification',
        token='your-slack-token',
        channel='#your-channel',
        text=message,
        username='Airflow',
        icon_emoji=':airflow:',
        dag=dag
    ).execute(context=kwargs)

with DAG(
    dag_id='mondo_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'color': 'default_color',
        'spark_jar': 'path/to/spark_jar',
        'obo_parser_spark_jar': 'path/to/obo_parser_spark_jar'
    }
) as dag:

    start = DummyOperator(task_id='start')

    validate_params = PythonOperator(
        task_id='params_validate',
        python_callable=params_validate,
        provide_context=True
    )

    check_s3_key = S3KeySensor(
        task_id='check_s3_key',
        bucket_key='mondo/mondo.obo',
        bucket_name='your-s3-bucket',
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600
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

    index_mondo = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo_terms,
        provide_context=True
    )

    publish_mondo_data = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        provide_context=True
    )

    send_slack_notification = PythonOperator(
        task_id='slack_notification',
        python_callable=slack_notification,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> validate_params >> check_s3_key >> download_mondo >> normalize_mondo >> index_mondo >> publish_mondo_data >> send_slack_notification >> end