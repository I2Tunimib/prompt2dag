from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
import requests
import json
import os
import logging

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Valid colors for validation
VALID_COLORS = {'red', 'green', 'blue', 'yellow', 'purple'}

# GitHub repository info
GITHUB_REPO = 'monarch-initiative/mondo'
GITHUB_API_URL = f'https://api.github.com/repos/{GITHUB_REPO}/releases/latest'

def validate_color_parameters(**context):
    """Validate the color parameter provided to the pipeline."""
    color = context['dag_run'].conf.get('color', 'green')  # default to green
    if color not in VALID_COLORS:
        raise ValueError(f"Invalid color: {color}. Must be one of {VALID_COLORS}")
    logging.info(f"Color parameter validated: {color}")
    return color

def download_mondo_terms(**context):
    """Downloads the latest Mondo OBO file, checks version, and uploads to S3."""
    # Get parameters
    color = context['task_instance'].xcom_pull(task_ids='params_validate')
    s3_bucket = context['dag_run'].conf.get('s3_bucket', 'mondo-ontology-data')
    s3_key_prefix = context['dag_run'].conf.get('s3_key_prefix', 'raw')
    
    # Check latest release on GitHub
    response = requests.get(GITHUB_API_URL)
    response.raise_for_status()
    release_data = response.json()
    latest_version = release_data['tag_name']
    
    # Check if we already have this version
    s3_hook = S3Hook(aws_conn_id='aws_default')
    version_key = f"{s3_key_prefix}/current_version.txt"
    
    existing_version = None
    if s3_hook.check_for_key(version_key, bucket_name=s3_bucket):
        existing_version = s3_hook.read_key(version_key, bucket_name=s3_bucket).strip()
    
    if existing_version == latest_version:
        logging.info(f"Version {latest_version} already exists. Skipping download.")
        context['task_instance'].xcom_push(key='version', value=latest_version)
        context['task_instance'].xcom_push(key='skipped', value=True)
        return "skip_processing"
    
    # Download the OBO file
    obo_asset = None
    for asset in release_data['assets']:
        if asset['name'].endswith('.obo'):
            obo_asset = asset
            break
    
    if not obo_asset:
        raise ValueError("No OBO file found in latest release")
    
    obo_url = obo_asset['browser_download_url']
    logging.info(f"Downloading Mondo OBO from {obo_url}")
    
    obo_response = requests.get(obo_url)
    obo_response.raise_for_status()
    
    # Upload to S3
    s3_key = f"{s3_key_prefix}/mondo_{latest_version}.obo"
    s3_hook.load_string(
        string_data=obo_response.text,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    # Update version file
    s3_hook.load_string(
        string_data=latest_version,
        key=version_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    logging.info(f"Uploaded Mondo OBO version {latest_version} to s3://{s3_bucket}/{s3_key}")
    
    # Push version info for downstream tasks
    context['task_instance'].xcom_push(key='version', value=latest_version)
    context['task_instance'].xcom_push(key='s3_path', value=f"s3://{s3_bucket}/{s3_key}")
    context['task_instance'].xcom_push(key='skipped', value=False)
    
    return "proceed"

def check_skip_condition(**context):
    """Branch operator to determine if processing should continue."""
    skipped = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='skipped')
    if skipped:
        return "skip_processing"
    return "normalized_mondo_terms"

def normalized_mondo_terms(**context):
    """Process Mondo OBO file using Spark to normalize and transform ontology terms."""
    # Get parameters
    spark_jar = context['dag_run'].conf.get('spark_jar', '/opt/spark/jars/mondo-normalizer.jar')
    s3_bucket = context['dag_run'].conf.get('s3_bucket', 'mondo-ontology-data')
    processed_prefix = context['dag_run'].conf.get('processed_prefix', 'processed')
    
    # Get version info from upstream
    version = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='version')
    s3_path = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='s3_path')
    
    if not version or not s3_path:
        raise ValueError("Missing version or S3 path from download task")
    
    logging.info(f"Processing Mondo version {version} from {s3_path}")
    
    # Simulate Spark processing (in real scenario, use SparkSubmitOperator)
    # Here we'll just create a mock normalized data file
    import tempfile
    
    # Mock normalized data
    normalized_data = {
        "version": version,
        "terms": [
            {"id": "MONDO:0000001", "name": "disease", "definition": "A disease entity"},
            {"id": "MONDO:0000002", "name": "cancer", "definition": "A type of disease"}
        ],
        "metadata": {
            "processed_at": datetime.utcnow().isoformat(),
            "color": context['task_instance'].xcom_pull(task_ids='params_validate')
        }
    }
    
    # Write to temp file and upload to S3
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(normalized_data, f)
        temp_file = f.name
    
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        output_key = f"{processed_prefix}/mondo_normalized_{version}.json"
        s3_hook.load_file(
            filename=temp_file,
            key=output_key,
            bucket_name=s3_bucket,
            replace=True
        )
        
        context['task_instance'].xcom_push(key='processed_s3_path', value=f"s3://{s3_bucket}/{output_key}")
        logging.info(f"Normalized data uploaded to s3://{s3_bucket}/{output_key}")
    finally:
        os.unlink(temp_file)
    
    return "proceed"

def index_mondo_terms(**context):
    """Indexes the normalized Mondo terms to Elasticsearch using Spark."""
    # Get parameters
    obo_parser_spark_jar = context['dag_run'].conf.get('obo_parser_spark_jar', '/opt/spark/jars/elasticsearch-indexer.jar')
    es_index = context['dag_run'].conf.get('es_index', 'mondo_terms')
    
    # Get processed data path
    processed_s3_path = context['task_instance'].xcom_pull(task_ids='normalized_mondo_terms', key='processed_s3_path')
    version = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='version')
    
    if not processed_s3_path:
        raise ValueError("Missing processed S3 path from normalization task")
    
    logging.info(f"Indexing Mondo version {version} to Elasticsearch from {processed_s3_path}")
    
    # Simulate Elasticsearch indexing (in real scenario, use SparkSubmitOperator)
    # Here we'll use Elasticsearch hook to index mock data
    es_hook = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    es_client = es_hook.get_conn()
    
    # Create index if it doesn't exist
    index_name = f"{es_index}_{version.replace('.', '_')}"
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text"},
                        "definition": {"type": "text"},
                        "version": {"type": "keyword"}
                    }
                }
            }
        )
        logging.info(f"Created Elasticsearch index: {index_name}")
    
    # Index sample documents (in real scenario, this would be done via Spark)
    # For demonstration, we'll index a few documents directly
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket, key = processed_s3_path.replace("s3://", "").split("/", 1)
    
    if s3_hook.check_for_key(key, bucket_name=bucket):
        file_content = s3_hook.read_key(key, bucket_name=bucket)
        data = json.loads(file_content)
        
        # Index terms
        for term in data.get('terms', []):
            doc = {
                **term,
                "version": version,
                "color": context['task_instance'].xcom_pull(task_ids='params_validate')
            }
            es_client.index(index=index_name, body=doc)
        
        logging.info(f"Indexed {len(data.get('terms', []))} terms to {index_name}")
    
    context['task_instance'].xcom_push(key='es_index', value=index_name)
    return "proceed"

def publish_mondo(**context):
    """Publishes the indexed Mondo data to make it available for consumption."""
    # Get parameters
    version = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='version')
    es_index = context['task_instance'].xcom_pull(task_ids='index_mondo_terms', key='es_index')
    
    if not version or not es_index:
        raise ValueError("Missing version or ES index from upstream tasks")
    
    logging.info(f"Publishing Mondo version {version} from index {es_index}")
    
    # In a real scenario, this might update a metadata store or create an alias
    # For demonstration, we'll just log the publication
    es_hook = ElasticsearchHook(elasticsearch_conn_id='elasticsearch_default')
    es_client = es_hook.get_conn()
    
    # Create or update alias to point to the new index
    alias_name = "mondo_current"
    try:
        # Remove old alias if exists
        if es_client.indices.exists_alias(name=alias_name):
            old_indices = es_client.indices.get_alias(name=alias_name).keys()
            for old_index in old_indices:
                es_client.indices.delete_alias(index=old_index, name=alias_name)
        
        # Add new alias
        es_client.indices.put_alias(index=es_index, name=alias_name)
        logging.info(f"Updated alias {alias_name} to point to {es_index}")
    except Exception as e:
        logging.warning(f"Could not update alias: {e}")
    
    context['task_instance'].xcom_push(key='published_version', value=version)
    return "proceed"

def send_slack_notification(**context):
    """Sends a Slack notification indicating pipeline completion."""
    # Get parameters
    slack_webhook = context['dag_run'].conf.get('slack_webhook')
    if not slack_webhook:
        logging.warning("No Slack webhook provided, skipping notification")
        return
    
    # Get execution status
    version = context['task_instance'].xcom_pull(task_ids='publish_mondo', key='published_version')
    color = context['task_instance'].xcom_pull(task_ids='params_validate')
    
    # Determine if pipeline was skipped
    skipped = context['task_instance'].xcom_pull(task_ids='download_mondo_terms', key='skipped')
    
    if skipped:
        message = f":information_source: Mondo pipeline skipped - version already up-to-date"
    else:
        message = f":white_check_mark: Mondo pipeline completed successfully!\nVersion: {version}\nColor: {color}"
    
    # Send notification
    slack_op = SlackWebhookOperator(
        task_id='slack_notification',
        webhook_token=slack_webhook,
        message=message,
        username='Airflow',
        dag=context['dag']
    )
    slack_op.execute(context=context)

def send_failure_notification(context):
    """Callback function to send Slack notification on failure."""
    slack_webhook = context['dag_run'].conf.get('slack_webhook')
    if not slack_webhook:
        logging.warning("No Slack webhook provided, skipping failure notification")
        return
    
    task_instance = context['task_instance']
    message = f":x: Mondo pipeline failed!\nTask: {task_instance.task_id}\nDag: {context['dag'].dag_id}\nExecution Date: {context['execution_date']}"
    
    slack_op = SlackWebhookOperator(
        task_id='slack_failure_notification',
        webhook_token=slack_webhook,
        message=message,
        username='Airflow',
        dag=context['dag']
    )
    slack_op.execute(context=context)

# Define the DAG
with DAG(
    dag_id='mondo_ontology_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mondo ontology data from Monarch Initiative',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks_per_dag=1,
    on_failure_callback=send_failure_notification,
    tags=['mondo', 'ontology', 'etl'],
) as dag:
    
    # Task 1: Validate parameters
    task_params_validate = PythonOperator(
        task_id='params_validate',
        python_callable=validate_color_parameters,
        provide_context=True,
    )
    
    # Task 2: Download Mondo terms
    task_download_mondo_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms,
        provide_context=True,
    )
    
    # Task 3: Branch to check if we should skip processing
    task_check_skip = BranchPythonOperator(
        task_id='check_skip_condition',
        python_callable=check_skip_condition,
        provide_context=True,
    )
    
    # Task 4: Normalized Mondo terms
    task_normalized_mondo_terms = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalized_mondo_terms,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Task 5: Index Mondo terms
    task_index_mondo_terms = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo_terms,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Task 6: Publish Mondo
    task_publish_mondo = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Task 7: Slack notification
    task_slack = PythonOperator(
        task_id='slack',
        python_callable=send_slack_notification,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Dummy task for skipping
    task_skip_processing = PythonOperator(
        task_id='skip_processing',
        python_callable=lambda **ctx: logging.info("Skipping processing - data already up-to-date"),
        provide_context=True,
    )
    
    # Define dependencies
    task_params_validate >> task_download_mondo_terms >> task_check_skip
    task_check_skip >> [task_normalized_mondo_terms, task_skip_processing]
    task_normalized_mondo_terms >> task_index_mondo_terms >> task_publish_mondo >> task_slack
    task_skip_processing >> task_slack