from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.trigger_rule import TriggerRule
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_notification(
        slack_conn_id='slack_default',
        text=":red_circle: Mondo Pipeline Failed on task {{ task_instance.task_id }}",
        channel="#data-pipelines"
    ),
}

# DAG definition
with DAG(
    dag_id='mondo_ontology_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mondo ontology data from Monarch Initiative',
    schedule_interval=None,  # Manual execution only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['mondo', 'ontology', 'etl', 'spark', 'elasticsearch'],
    params={
        'color': 'blue',
        'spark_jar': 's3://my-bucket/spark-jobs/mondo-normalization.jar',
        'obo_parser_spark_jar': 's3://my-bucket/spark-jobs/obo-parser.jar',
    },
    doc_md="""
    # Mondo Ontology ETL Pipeline
    
    This pipeline imports Mondo ontology data from the Monarch Initiative repository.
    
    **Steps:**
    1. Validate parameters
    2. Download Mondo OBO file from GitHub
    3. Normalize terms using Spark
    4. Index to Elasticsearch
    5. Publish the data
    6. Send Slack notification
    
    **Parameters:**
    - color: Color parameter for pipeline execution
    - spark_jar: Path to Spark jar for normalization
    - obo_parser_spark_jar: Path to OBO parser Spark jar
    """
) as dag:

    def validate_params_task(**context):
        """Validate pipeline parameters."""
        params = context['params']
        color = params.get('color')
        
        if not color:
            raise ValueError("Color parameter is required")
        
        valid_colors = ['blue', 'green', 'red', 'yellow']
        if color not in valid_colors:
            raise ValueError(f"Invalid color: {color}. Must be one of {valid_colors}")
        
        logger.info(f"Parameters validated successfully. Color: {color}")
        return True

    def download_mondo_terms_task(**context):
        """Download latest Mondo OBO file and upload to S3 if newer version available."""
        import requests
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        # GitHub API for latest release
        repo = "monarch-initiative/mondo"
        api_url = f"https://api.github.com/repos/{repo}/releases/latest"
        
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            release_data = response.json()
            
            # Find OBO file asset
            obo_asset = None
            for asset in release_data.get('assets', []):
                if asset['name'].endswith('.obo'):
                    obo_asset = asset
                    break
            
            if not obo_asset:
                logger.warning("No OBO file found in latest release")
                context['task_instance'].xcom_push(key='skip_processing', value=True)
                return "skip_processing"
            
            # Check version against existing S3 file
            s3_hook = S3Hook(aws_conn_id='aws_default')
            bucket_name = 'mondo-ontology-data'
            s3_key = f"raw/mondo_{release_data['tag_name']}.obo"
            
            if s3_hook.check_for_key(s3_key, bucket_name):
                logger.info(f"Version {release_data['tag_name']} already exists in S3")
                context['task_instance'].xcom_push(key='skip_processing', value=True)
                return "skip_processing"
            
            # Download and upload to S3
            download_url = obo_asset['browser_download_url']
            logger.info(f"Downloading {download_url}")
            
            file_response = requests.get(download_url)
            file_response.raise_for_status()
            
            # Upload to S3
            s3_hook.load_string(
                string_data=file_response.content,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            # Store version info for downstream tasks
            context['task_instance'].xcom_push(key='mondo_version', value=release_data['tag_name'])
            context['task_instance'].xcom_push(key='s3_path', value=f"s3://{bucket_name}/{s3_key}")
            context['task_instance'].xcom_push(key='skip_processing', value=False)
            
            logger.info(f"Successfully downloaded and uploaded Mondo {release_data['tag_name']}")
            return "proceed_to_normalization"
            
        except Exception as e:
            logger.error(f"Error downloading Mondo terms: {e}")
            raise

    def check_if_processing_needed_task(**context):
        """Check if normalization step should be skipped."""
        ti = context['task_instance']
        skip_processing = ti.xcom_pull(task_ids='download_mondo_terms', key='skip_processing')
        
        if skip_processing:
            logger.info("Skipping normalization step")
            return "skip_normalization"
        return "normalized_mondo_terms"

    def normalized_mondo_terms_task(**context):
        """Normalize Mondo terms using Spark."""
        from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
        from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
        
        ti = context['task_instance']
        s3_path = ti.xcom_pull(task_ids='download_mondo_terms', key='s3_path')
        mondo_version = ti.xcom_pull(task_ids='download_mondo_terms', key='mondo_version')
        
        if not s3_path or not mondo_version:
            raise ValueError("Missing required XCom values from download task")
        
        spark_jar = context['params']['spark_jar']
        
        # Spark job configuration
        spark_steps = [{
            'Name': 'Normalize Mondo Terms',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '--class', 'com.example.MondoNormalizationJob',
                    spark_jar,
                    '--input', s3_path,
                    '--output', f"s3://mondo-ontology-data/processed/mondo_{mondo_version}/",
                    '--color', context['params']['color']
                ]
            }
        }]
        
        # This is a simplified version - in production you'd use EMR operators
        logger.info(f"Would run Spark job with steps: {spark_steps}")
        
        # Simulate processing
        output_path = f"s3://mondo-ontology-data/processed/mondo_{mondo_version}/"
        ti.xcom_push(key='processed_path', value=output_path)
        
        return output_path

    def check_if_indexing_needed_task(**context):
        """Check if indexing step should be skipped."""
        ti = context['task_instance']
        skip_processing = ti.xcom_pull(task_ids='download_mondo_terms', key='skip_processing')
        
        if skip_processing:
            logger.info("Skipping indexing step")
            return "skip_indexing"
        return "index_mondo_terms"

    def index_mondo_terms_task(**context):
        """Index normalized Mondo terms to Elasticsearch."""
        ti = context['task_instance']
        processed_path = ti.xcom_pull(task_ids='normalized_mondo_terms', key='processed_path')
        
        if not processed_path:
            raise ValueError("Missing processed path from normalization task")
        
        spark_jar = context['params']['obo_parser_spark_jar']
        
        # Spark job for Elasticsearch indexing
        spark_steps = [{
            'Name': 'Index Mondo to Elasticsearch',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '--class', 'com.example.MondoElasticsearchIndexer',
                    spark_jar,
                    '--input', processed_path,
                    '--es-index', 'mondo_ontology',
                    '--color', context['params']['color']
                ]
            }
        }]
        
        logger.info(f"Would run Spark indexing job with steps: {spark_steps}")
        
        # Simulate indexing
        ti.xcom_push(key='es_index', value='mondo_ontology')
        
        return "Indexing completed"

    def publish_mondo_task(**context):
        """Publish the indexed Mondo data."""
        ti = context['task_instance']
        es_index = ti.xcom_pull(task_ids='index_mondo_terms', key='es_index')
        mondo_version = ti.xcom_pull(task_ids='download_mondo_terms', key='mondo_version')
        
        # In production, this might update a metadata store or API
        logger.info(f"Publishing Mondo data - Version: {mondo_version}, ES Index: {es_index}")
        
        # Simulate publishing
        published_url = f"https://data.example.com/mondo/{mondo_version}"
        ti.xcom_push(key='published_url', value=published_url)
        
        return published_url

    def send_slack_notification_task(**context):
        """Send Slack notification on pipeline completion."""
        ti = context['task_instance']
        mondo_version = ti.xcom_pull(task_ids='download_mondo_terms', key='mondo_version')
        published_url = ti.xcom_pull(task_ids='publish_mondo', key='published_url')
        
        if mondo_version and published_url:
            message = f"""
            :white_check_mark: Mondo Ontology Pipeline Completed Successfully!
            
            *Version:* {mondo_version}
            *Published URL:* {published_url}
            *Color:* {context['params']['color']}
            """
        else:
            message = """
            :white_check_mark: Mondo Ontology Pipeline Completed!
            (No new data was processed)
            """
        
        return message

    # Define tasks
    start = EmptyOperator(task_id='start')
    
    validate_params = PythonOperator(
        task_id='params_validate',
        python_callable=validate_params_task,
        provide_context=True
    )
    
    download_mondo_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms_task,
        provide_context=True
    )
    
    check_if_processing_needed = BranchPythonOperator(
        task_id='check_if_processing_needed',
        python_callable=check_if_processing_needed_task,
        provide_context=True
    )
    
    normalized_mondo_terms = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalized_mondo_terms_task,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    skip_normalization = EmptyOperator(task_id='skip_normalization')
    
    check_if_indexing_needed = BranchPythonOperator(
        task_id='check_if_indexing_needed',
        python_callable=check_if_indexing_needed_task,
        provide_context=True
    )
    
    index_mondo_terms = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo_terms_task,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    skip_indexing = EmptyOperator(task_id='skip_indexing')
    
    publish_mondo = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo_task,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    slack_notification = PythonOperator(
        task_id='slack',
        python_callable=send_slack_notification_task,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED)

    # Define dependencies
    start >> validate_params >> download_mondo_terms >> check_if_processing_needed
    
    check_if_processing_needed >> normalized_mondo_terms >> check_if_indexing_needed
    check_if_processing_needed >> skip_normalization >> check_if_indexing_needed
    
    check_if_indexing_needed >> index_mondo_terms >> publish_mondo
    check_if_indexing_needed >> skip_indexing >> publish_mondo
    
    publish_mondo >> slack_notification >> end