import logging
import os
import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# DAG definition
with DAG(
    dag_id="mondo_ontology_etl",
    default_args=default_args,
    description="ETL pipeline for Mondo ontology data",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["etl", "mondo", "ontology"],
) as dag:

    def validate_params(**context):
        """
        Validate pipeline parameters passed via dag_run.conf.
        Expected parameters:
            - color: str (optional, default "blue")
            - spark_jar: str (required)
            - obo_parser_spark_jar: str (required)
        """
        conf = context.get("dag_run").conf or {}
        color = conf.get("color", "blue")
        spark_jar = conf.get("spark_jar")
        obo_parser_spark_jar = conf.get("obo_parser_spark_jar")

        if not spark_jar:
            raise ValueError("Parameter 'spark_jar' must be provided.")
        if not obo_parser_spark_jar:
            raise ValueError("Parameter 'obo_parser_spark_jar' must be provided.")

        # Store validated params in XCom for downstream tasks
        context["ti"].xcom_push(key="color", value=color)
        context["ti"].xcom_push(key="spark_jar", value=spark_jar)
        context["ti"].xcom_push(key="obo_parser_spark_jar", value=obo_parser_spark_jar)

    params_validate = PythonOperator(
        task_id="params_validate",
        python_callable=validate_params,
        provide_context=True,
    )

    def download_mondo_terms(**context):
        """
        Download the latest Mondo OBO file from GitHub releases.
        If a newer version is available compared to the version stored in S3,
        upload the new file to S3 and push the version to XCom.
        """
        s3_bucket = os.getenv("MONDO_S3_BUCKET", "my-mondo-bucket")
        s3_key_prefix = "raw/mondo"
        s3 = S3Hook(aws_conn_id="aws_default")

        # Get latest release info from GitHub
        api_url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        release_info = response.json()
        latest_version = release_info.get("tag_name")
        assets = release_info.get("assets", [])
        obo_asset = next((a for a in assets if a["name"].endswith(".obo")), None)

        if not obo_asset:
            raise ValueError("No OBO asset found in the latest release.")

        # Determine currently stored version
        version_key = f"{s3_key_prefix}/version.txt"
        try:
            current_version_obj = s3.get_key(key=version_key, bucket_name=s3_bucket)
            current_version = current_version_obj.get()["Body"].read().decode().strip()
        except Exception:
            current_version = None

        if current_version == latest_version:
            logging.info("Mondo OBO file is up‑to‑date (version %s). Skipping download.", latest_version)
            raise AirflowSkipException("No new version available.")

        # Download OBO file
        download_url = obo_asset["browser_download_url"]
        obo_response = requests.get(download_url, timeout=60)
        obo_response.raise_for_status()
        obo_content = obo_response.content

        # Upload to S3
        obo_key = f"{s3_key_prefix}/{latest_version}/mondo.obo"
        s3.load_string(string_data=obo_content.decode(), key=obo_key, bucket_name=s3_bucket, replace=True)
        s3.load_string(string_data=latest_version, key=version_key, bucket_name=s3_bucket, replace=True)

        logging.info("Uploaded Mondo OBO version %s to s3://%s/%s", latest_version, s3_bucket, obo_key)

        # Push version to XCom for downstream tasks
        context["ti"].xcom_push(key="mondo_version", value=latest_version)
        context["ti"].xcom_push(key="obo_s3_path", value=f"s3://{s3_bucket}/{obo_key}")

    download_mondo_terms = PythonOperator(
        task_id="download_mondo_terms",
        python_callable=download_mondo_terms,
        provide_context=True,
    )

    def publish_success(**context):
        """
        Publish a success message to Slack.
        """
        logging.info("Mondo ETL pipeline completed successfully.")

    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        http_conn_id="slack_default",
        webhook_token=None,  # token is stored in the connection
        message="✅ Mondo ontology ETL pipeline completed successfully.",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def notify_failure(context):
        """
        Send a Slack notification when any task fails.
        """
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        message = (
            f"❌ Task *{task_instance.task_id}* in DAG *{dag_run.dag_id}* failed.\n"
            f"Execution date: {task_instance.execution_date}\n"
            f"Log URL: {task_instance.log_url}"
        )
        return SlackWebhookOperator(
            task_id="slack_failure",
            http_conn_id="slack_default",
            webhook_token=None,
            message=message,
            trigger_rule=TriggerRule.ONE_FAILED,
        ).execute(context=context)

    # Spark job to normalize Mondo terms
    normalized_mondo_terms = SparkSubmitOperator(
        task_id="normalized_mondo_terms",
        application="{{ ti.xcom_pull(key='spark_jar') }}",
        name="normalize_mondo",
        conn_id="spark_default",
        application_args=[
            "--input",
            "{{ ti.xcom_pull(key='obo_s3_path') }}",
            "--output",
            "s3://{{ var.value.mondo_processed_bucket }}/normalized/",
            "--parser-jar",
            "{{ ti.xcom_pull(key='obo_parser_spark_jar') }}",
        ],
        conf={"spark.driver.extraClassPath": "{{ ti.xcom_pull(key='obo_parser_spark_jar') }}"},
        execution_timeout=timedelta(hours=2),
    )

    # Spark job to index normalized terms into Elasticsearch
    index_mondo_terms = SparkSubmitOperator(
        task_id="index_mondo_terms",
        application="{{ ti.xcom_pull(key='spark_jar') }}",
        name="index_mondo",
        conn_id="spark_default",
        application_args=[
            "--input",
            "s3://{{ var.value.mondo_processed_bucket }}/normalized/",
            "--es-index",
            "mondo",
            "--es-host",
            "{{ var.value.elasticsearch_host }}",
        ],
        execution_timeout=timedelta(hours=2),
    )

    def publish_mondo(**context):
        """
        Publish the indexed Mondo data (e.g., update an alias in Elasticsearch).
        """
        es_host = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
        es = ElasticsearchHook(conn_id="elasticsearch_default")
        alias_name = "mondo_current"
        index_name = "mondo"

        # Simple alias switch logic
        es.client.indices.put_alias(index=index_name, name=alias_name)
        logging.info("Alias %s now points to index %s", alias_name, index_name)

    publish_mondo = PythonOperator(
        task_id="publish_mondo",
        python_callable=publish_mondo,
        provide_context=True,
    )

    # Failure notification task (runs on any upstream failure)
    slack_failure = PythonOperator(
        task_id="slack_failure",
        python_callable=notify_failure,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Define task dependencies
    params_validate >> download_mondo_terms >> normalized_mondo_terms >> index_mondo_terms >> publish_mondo >> slack_success
    # Failure notification should listen to all main tasks
    [params_validate, download_mondo_terms, normalized_mondo_terms, index_mondo_terms, publish_mondo] >> slack_failure