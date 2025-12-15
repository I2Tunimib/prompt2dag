# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: etl_import_mondo
Description: Comprehensive Pipeline Description
Generated on: 2024-06-13
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# -------------------------------------------------------------------------
# Default arguments and DAG definition
# -------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# The DAG is disabled (no schedule). It can be triggered manually.
with DAG(
    dag_id="etl_import_mondo",
    description="Comprehensive Pipeline Description",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "mondo"],
    max_active_runs=1,
) as dag:
    # -------------------------------------------------------------------------
    # Helper: Slack failure notification
    # -------------------------------------------------------------------------
    def _slack_notify(message: str) -> None:
        """Send a message to the configured Slack webhook."""
        webhook_url = Variable.get("slack_webhook", default_var=None)
        if not webhook_url:
            logging.warning("Slack webhook URL not defined in Airflow Variable 'slack_webhook'.")
            return

        payload = {"text": message}
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logging.info("Slack notification sent.")
        except Exception as exc:  # pragma: no cover
            logging.error("Failed to send Slack notification: %s", exc)

    def _failure_callback(context):
        """Airflow failure callback that posts to Slack."""
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        execution_date = context.get("execution_date")
        log_url = context.get("task_instance").log_url

        message = (
            f":red_circle: *Task Failed*\n"
            f"*DAG*: `{dag_id}`\n"
            f"*Task*: `{task_id}`\n"
            f"*Execution*: `{execution_date}`\n"
            f"*Log*: {log_url}"
        )
        _slack_notify(message)

    # -------------------------------------------------------------------------
    # Task: Validate Pipeline Parameters
    # -------------------------------------------------------------------------
    @task(task_id="validate_params", retries=0, on_failure_callback=_failure_callback)
    def validate_params() -> None:
        """
        Validate required Airflow Variables and Connections.
        Raises AirflowFailException if validation fails.
        """
        required_vars = ["github_mondo_releases", "s3_datalake", "elasticsearch_mondo"]
        missing = [var for var in required_vars if not Variable.get(var, default_var=None)]
        if missing:
            raise AirflowFailException(f"Missing required Airflow Variables: {missing}")

        # Example of connection validation (will raise if not found)
        from airflow.hooks.base import BaseHook

        for conn_id in ["github_mondo_releases", "s3_datalake", "elasticsearch_mondo", "slack_webhook"]:
            try:
                BaseHook.get_connection(conn_id)
            except Exception as exc:
                raise AirflowFailException(f"Connection '{conn_id}' not found: {exc}")

        logging.info("All required parameters and connections are present.")

    # -------------------------------------------------------------------------
    # Task: Download Mondo OBO File
    # -------------------------------------------------------------------------
    @task(task_id="download_mondo_terms", retries=0, on_failure_callback=_failure_callback)
    def download_mondo_terms() -> str:
        """
        Download the latest Mondo OBO file from GitHub releases and upload to S3.
        Returns the S3 key of the uploaded file.
        """
        # Resolve GitHub releases connection
        from airflow.hooks.base import BaseHook

        gh_conn = BaseHook.get_connection("github_mondo_releases")
        token = gh_conn.password or gh_conn.login  # token may be stored in password field
        headers = {"Authorization": f"token {token}"} if token else {}

        # GitHub API to fetch latest release assets
        api_url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        release_data = response.json()

        # Find OBO asset
        obo_asset = next(
            (a for a in release_data["assets"] if a["name"].endswith(".obo")),
            None,
        )
        if not obo_asset:
            raise AirflowFailException("No OBO file found in the latest Mondo release.")

        download_url = obo_asset["browser_download_url"]
        logging.info("Downloading OBO file from %s", download_url)
        obo_response = requests.get(download_url, headers=headers, timeout=60)
        obo_response.raise_for_status()
        obo_content = obo_response.content

        # Upload to S3
        s3_conn = BaseHook.get_connection("s3_datalake")
        s3 = boto3.session.Session(
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            region_name=s3_conn.extra_dejson.get("region_name", "us-east-1"),
        ).resource("s3")
        bucket_name = s3_conn.schema  # Expected format: bucket-name
        if not bucket_name:
            raise AirflowFailException("S3 bucket name not defined in connection schema.")

        s3_key = f"mondo/raw/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.obo"
        obj = s3.Object(bucket_name, s3_key)
        obj.put(Body=obo_content)
        logging.info("Uploaded OBO file to s3://%s/%s", bucket_name, s3_key)

        return s3_key

    # -------------------------------------------------------------------------
    # Task: Normalize Mondo Terms (Spark)
    # -------------------------------------------------------------------------
    normalize_mondo_terms = SparkSubmitOperator(
        task_id="normalize_mondo_terms",
        application="/opt/airflow/dags/spark/normalize_mondo.py",
        name="normalize_mondo_terms",
        conn_id="spark_default",
        application_args=[
            "--input-s3-key", "{{ ti.xcom_pull(task_ids='download_mondo_terms') }}",
            "--output-s3-key", "mondo/normalized/normalized_terms.parquet",
        ],
        conf={"spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
        retries=0,
        on_failure_callback=_failure_callback,
    )

    # -------------------------------------------------------------------------
    # Task: Index Mondo Terms into Elasticsearch (Spark)
    # -------------------------------------------------------------------------
    index_mondo_terms = SparkSubmitOperator(
        task_id="index_mondo_terms",
        application="/opt/airflow/dags/spark/index_mondo.py",
        name="index_mondo_terms",
        conn_id="spark_default",
        application_args=[
            "--input-s3-key", "mondo/normalized/normalized_terms.parquet",
            "--es-index", "mondo",
            "--es-host", "{{ conn.elasticsearch_mondo.host }}",
            "--es-port", "{{ conn.elasticsearch_mondo.port }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.es.nodes": "{{ conn.elasticsearch_mondo.host }}",
            "spark.es.port": "{{ conn.elasticsearch_mondo.port }}",
        },
        retries=0,
        on_failure_callback=_failure_callback,
    )

    # -------------------------------------------------------------------------
    # Task: Publish Mondo Dataset
    # -------------------------------------------------------------------------
    @task(task_id="publish_mondo", retries=0, on_failure_callback=_failure_callback)
    def publish_mondo() -> None:
        """
        Move the normalized dataset to a public location in the data lake.
        """
        from airflow.hooks.base import BaseHook

        s3_conn = BaseHook.get_connection("s3_datalake")
        s3 = boto3.session.Session(
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            region_name=s3_conn.extra_dejson.get("region_name", "us-east-1"),
        ).resource("s3")
        bucket_name = s3_conn.schema
        source_key = "mondo/normalized/normalized_terms.parquet"
        dest_key = f"mondo/published/{datetime.utcnow().strftime('%Y%m%d')}/normalized_terms.parquet"

        copy_source = {"Bucket": bucket_name, "Key": source_key}
        s3.Object(bucket_name, dest_key).copy(copy_source)
        logging.info("Published dataset to s3://%s/%s", bucket_name, dest_key)

    # -------------------------------------------------------------------------
    # Task: Slack Notification
    # -------------------------------------------------------------------------
    @task(task_id="notify_slack", retries=0, trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_slack() -> None:
        """
        Send a success notification to Slack.
        """
        message = (
            ":white_check_mark: *Mondo ETL Pipeline Completed Successfully*\n"
            f"*DAG*: `etl_import_mondo`\n"
            f"*Run*: `{datetime.utcnow().isoformat()}`"
        )
        _slack_notify(message)

    # -------------------------------------------------------------------------
    # Define task dependencies (sequential pattern)
    # -------------------------------------------------------------------------
    validate = validate_params()
    download = download_mondo_terms()
    validate >> download >> normalize_mondo_terms >> index_mondo_terms >> publish_mondo() >> notify_slack()