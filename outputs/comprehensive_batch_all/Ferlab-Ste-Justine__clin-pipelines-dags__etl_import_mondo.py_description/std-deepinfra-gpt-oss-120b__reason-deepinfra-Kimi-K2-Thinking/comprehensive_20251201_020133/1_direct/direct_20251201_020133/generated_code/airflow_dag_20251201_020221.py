import os
import json
import logging
import requests
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Default arguments
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # will be set after function definition
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# Failure notification callback
# ----------------------------------------------------------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def _notify_slack_failure(context):
    """Send a Slack message when a task fails."""
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL not set; skipping failure notification.")
        return

    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    log_url = task.log_url if task else "N/A"

    message = (
        f":red_circle: Task *{task.task_id}* in DAG *{dag_id}* failed.\n"
        f"*Execution date*: {execution_date}\n"
        f"*Log URL*: {log_url}"
    )
    try:
        requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
    except Exception as exc:  # pragma: no cover
        logging.error("Failed to send Slack failure notification: %s", exc)


default_args["on_failure_callback"] = _notify_slack_failure

# ----------------------------------------------------------------------
# Helper functions for tasks
# ----------------------------------------------------------------------
ALLOWED_COLORS = {"red", "green", "blue", "yellow"}


def validate_params(**kwargs):
    """Validate the 'color' parameter."""
    color = kwargs.get("params", {}).get("color")
    if not color:
        raise ValueError("Parameter 'color' is required.")
    if color not in ALLOWED_COLORS:
        raise ValueError(f"Invalid color '{color}'. Allowed values: {ALLOWED_COLORS}")
    logging.info("Parameter validation passed. Color: %s", color)


def download_mondo(**kwargs):
    """Download the latest Mondo OBO file and upload to S3 if newer."""
    s3_bucket = os.getenv("S3_BUCKET", "my-mondo-bucket")
    s3_key_prefix = os.getenv("S3_KEY_PREFIX", "mondo")
    version_key = f"{s3_key_prefix}/mondo_version.txt"

    s3 = boto3.client("s3")
    # Get current version from S3 (if exists)
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=version_key)
        current_version = obj["Body"].read().decode().strip()
        logging.info("Current stored Mondo version: %s", current_version)
    except s3.exceptions.NoSuchKey:
        current_version = None
        logging.info("No existing version file found in S3.")

    # Fetch latest release info from GitHub
    api_url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
    resp = requests.get(api_url, timeout=10)
    resp.raise_for_status()
    release_info = resp.json()
    latest_version = release_info.get("tag_name")
    if not latest_version:
        raise RuntimeError("Unable to determine latest Mondo version from GitHub.")
    logging.info("Latest Mondo version from GitHub: %s", latest_version)

    if current_version == latest_version:
        logging.info("Mondo data is up‑to‑date. Skipping download.")
        return

    # Find the OBO asset
    obo_asset = None
    for asset in release_info.get("assets", []):
        if asset["name"].endswith(".obo"):
            obo_asset = asset
            break
    if not obo_asset:
        raise RuntimeError("No OBO asset found in the latest release.")

    download_url = obo_asset["browser_download_url"]
    logging.info("Downloading OBO file from %s", download_url)
    obo_resp = requests.get(download_url, timeout=30)
    obo_resp.raise_for_status()
    obo_content = obo_resp.content

    # Upload OBO file to S3
    obo_key = f"{s3_key_prefix}/mondo_{latest_version}.obo"
    s3.put_object(Bucket=s3_bucket, Key=obo_key, Body=obo_content)
    logging.info("Uploaded OBO file to s3://%s/%s", s3_bucket, obo_key)

    # Update version file
    s3.put_object(Bucket=s3_bucket, Key=version_key, Body=latest_version.encode())
    logging.info("Updated version file to %s", latest_version)


def publish_mondo(**kwargs):
    """Publish the indexed Mondo data (placeholder implementation)."""
    logging.info("Publishing Mondo data – placeholder implementation.")
    # In a real scenario, this could trigger an API call or update a catalog entry.


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="mondo_etl",
    description="ETL pipeline for Mondo ontology data",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "mondo"],
) as dag:

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------
    params_validate = PythonOperator(
        task_id="params_validate",
        python_callable=validate_params,
        provide_context=False,
    )

    download_mondo_terms = PythonOperator(
        task_id="download_mondo_terms",
        python_callable=download_mondo,
        provide_context=False,
    )

    normalized_mondo_terms = SparkSubmitOperator(
        task_id="normalized_mondo_terms",
        application="/usr/local/airflow/dags/scripts/normalize_mondo.py",
        name="normalize_mondo",
        conf={"spark.master": "local[*]"},
        jars="{{ params.spark_jar }}",
        application_args=[
            "--input",
            "s3://{{ var.value.get('S3_BUCKET', 'my-mondo-bucket') }}/mondo",
            "--output",
            "s3://{{ var.value.get('S3_BUCKET', 'my-mondo-bucket') }}/mondo_normalized",
        ],
        driver_memory="2g",
        executor_memory="2g",
        num_executors=2,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    index_mondo_terms = SparkSubmitOperator(
        task_id="index_mondo_terms",
        application="/usr/local/airflow/dags/scripts/index_mondo.py",
        name="index_mondo",
        conf={"spark.master": "local[*]"},
        jars="{{ params.spark_jar }}",
        application_args=[
            "--input",
            "s3://{{ var.value.get('S3_BUCKET', 'my-mondo-bucket') }}/mondo_normalized",
            "--es-host",
            "{{ var.value.get('ES_HOST', 'http://elasticsearch:9200') }}",
            "--es-index",
            "mondo",
        ],
        driver_memory="2g",
        executor_memory="2g",
        num_executors=2,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    publish_mondo = PythonOperator(
        task_id="publish_mondo",
        python_callable=publish_mondo,
        provide_context=False,
    )

    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        http_conn_id=None,
        webhook_token=SLACK_WEBHOOK_URL,
        message=":white_check_mark: Mondo ETL pipeline completed successfully.",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------
    (
        params_validate
        >> download_mondo_terms
        >> normalized_mondo_terms
        >> index_mondo_terms
        >> publish_mondo
        >> slack_success
    )