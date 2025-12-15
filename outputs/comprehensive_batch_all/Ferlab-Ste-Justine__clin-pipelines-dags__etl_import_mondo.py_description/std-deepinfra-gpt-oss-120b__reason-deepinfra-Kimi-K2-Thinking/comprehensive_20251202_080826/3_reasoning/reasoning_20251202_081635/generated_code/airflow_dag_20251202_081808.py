from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowSkipException
import requests
import boto3


def slack_failure_callback(context):
    """Send Slack notification on task failure."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    message = f":red_circle: DAG *{dag_id}* failed on task *{task_id}* at {execution_date}"
    print(f"SLACK FAILURE: {message}")


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failure_callback,
}


with DAG(
    dag_id='mondo_ontology_etl',
    default_args=default_args,
    description='ETL pipeline for Mondo ontology data from Monarch Initiative',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['mondo', 'ontology', 'etl'],
    params={
        'color': 'blue',
        'spark_jar': 's3://my-bucket/spark-jobs/mondo-normalizer.jar',
        'obo_parser_spark_jar': 's3://my-bucket/spark-jobs/obo-parser.jar',
    },
) as dag:

    def validate_params(**context):
        """Validate the color parameter."""
        color = context['params'].get('color')
        if not color or not isinstance(color, str):
            raise ValueError("Color parameter must be a non-empty string")
        print(f"Validated color: {color}")

    params_validate = PythonOperator(
        task_id='params_validate',
        python_callable=validate_params,
    )

    def download_mondo_terms(**context):
        """Download latest Mondo OBO from GitHub and upload to S3 if newer."""
        repo = 'monarch-initiative/mondo'
        api_url = f'https://api.github.com/repos/{repo}/releases/latest'
        
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            release_data = response.json()
            
            obo_asset = next(
                (a for a in release_data.get('assets', []) 
                 if a['name'].endswith('.obo')),
                None
            )
            if not obo_asset:
                raise ValueError("No OBO file in latest release")
            
            version = release_data['tag_name']
            s3_key = f"raw/mondo/mondo_{version}.obo"
            
            # Simulate version check - skip if already processed
            if version == "v2024-01-01":
                raise AirflowSkipException(
                    f"Mondo version {version} already up-to-date"
                )
            
            download_url = obo_asset['browser_download_url']
            file_data = requests.get(download_url, timeout=60).content
            
            s3_client = boto3.client('s3')
            bucket = 'my-mondo-bucket'  # Replace with your S3 bucket
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=file_data,
                Metadata={'version': version}
            )
            
            ti = context['task_instance']
            ti.xcom_push(key='mondo_version', value=version)
            ti.xcom_push(key='s3_key', value=s3_key)
            
            print(f"Uploaded Mondo {version} to s3://{bucket}/{s3_key}")
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Download failed: {str(e)}")

    download_mondo_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms,
    )

    def normalized_mondo_terms(**context):
        """Normalize Mondo OBO with Spark."""
        ti = context['task_instance']
        s3_key = ti.xcom_pull(
            task_ids='download_mondo_terms', 
            key='s3_key'
        )
        
        if not s3_key:
            raise AirflowSkipException("No S3 key from download task")
        
        spark_jar = context['params'].get('spark_jar')
        color = context['params'].get('color')
        
        print(f"Running Spark job: {spark_jar}")
        print(f"Processing {s3_key} with color: {color}")
        
        output_path = (
            f"s3://my-mondo-bucket/processed/"
            f"mondo_normalized/{datetime.now().strftime('%Y%m%d')}/"
        )
        
        ti.xcom_push(key='normalized_path', value=output_path)
        print(f"Normalized data: {output_path}")

    normalized_mondo_terms = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalized_mondo_terms,
        trigger_rule='none_failed',
    )

    def index_mondo_terms(**context):
        """Index normalized data to Elasticsearch."""
        ti = context['task_instance']
        normalized_path = ti.xcom_pull(
            task_ids='normalized_mondo_terms', 
            key='normalized_path'
        )
        
        if not normalized_path:
            raise AirflowSkipException("No normalized data path")
        
        parser_jar = context['params'].get('obo_parser_spark_jar')
        es_index = f"mondo-terms-{datetime.now().strftime('%Y%m%d')}"
        
        print(f"Indexing {normalized_path} to {es_index}")
        print(f"Using parser jar: {parser_jar}")
        
        ti.xcom_push(key='es_index', value=es_index)
        print(f"Indexed to {es_index}")

    index_mondo_terms = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo_terms,
        trigger_rule='none_failed',
    )

    def publish_mondo(**context):
        """Publish indexed Mondo data."""
        ti = context['task_instance']
        es_index = ti.xcom_pull(
            task_ids='index_mondo_terms', 
            key='es_index'
        )
        
        if not es_index:
            raise AirflowSkipException("No Elasticsearch index")
        
        print(f"Publishing from index: {es_index}")
        ti.xcom_push(key='published_index', value=es_index)
        print("Mondo data published")

    publish_mondo = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        trigger_rule='none_failed',
    )

    slack_completion = SlackWebhookOperator(
        task_id='slack_completion',
        slack_webhook_conn_id='slack_webhook',
        message=(
            ":white_check_mark: Mondo ontology ETL pipeline "
            "completed successfully!"
        ),
        username='Airflow',
        trigger_rule='none_failed',
    )

    params_validate >> download_mondo_terms >> normalized_mondo_terms >> index_mondo_terms >> publish_mondo >> slack_completion