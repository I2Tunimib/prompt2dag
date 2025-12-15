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

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="etl_import_mondo",
    description="Comprehensive Pipeline Description",
    schedule=None,                     # Disabled schedule
    start_date=datetime(2024, 1, 1, tzinfo=datetime.utcnow().tzinfo),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "mondo"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # ---------------------------------------------------------------------
    # Task: Validate Parameters
    # ---------------------------------------------------------------------
    @task(task_id="validate_params", retries=0)
    def validate_params() -> dict:
        """
        Validate input parameters required for the pipeline.
        Returns a dictionary of validated parameters.
        """
        try:
            # Example validation logic (replace with real checks)
            params = {
                "download_url": os.getenv("MONDO_DOWNLOAD_URL", "https://example.com/mondo.obo"),
                "spark_conn_id": os.getenv("SPARK_CONN_ID", "spark_default"),
                "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
                "output_path": os.getenv("MONDO_OUTPUT_PATH", "/tmp/mondo_normalized.parquet"),
                "index_path": os.getenv("MONDO_INDEX_PATH", "/tmp/mondo_index.parquet"),
                "slack_webhook": os.getenv("SLACK_WEBHOOK_URL"),
            }

            missing = [k for k, v in params.items() if v is None]
            if missing:
                raise ValueError(f"Missing required environment variables: {missing}")

            logging.info("Parameters validated successfully.")
            return params
        except Exception as exc:
            logging.error("Parameter validation failed: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Download Mondo OBO File
    # ---------------------------------------------------------------------
    @task(task_id="download_mondo_terms", retries=0)
    def download_mondo_terms(params: dict) -> str:
        """
        Downloads the Mondo OBO file to a local temporary location.
        Returns the local file path.
        """
        import tempfile

        url = params["download_url"]
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            tmp_dir = tempfile.mkdtemp()
            file_path = os.path.join(tmp_dir, "mondo.obo")
            with open(file_path, "wb") as f:
                f.write(response.content)
            logging.info("Mondo OBO file downloaded to %s", file_path)
            return file_path
        except Exception as exc:
            logging.error("Failed to download Mondo OBO file: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Normalize Mondo Terms (Spark)
    # ---------------------------------------------------------------------
    normalize_mondo_terms = SparkSubmitOperator(
        task_id="normalize_mondo_terms",
        application="/opt/airflow/dags/spark/normalize_mondo.py",  # placeholder path
        conn_id="{{ ti.xcom_pull(task_ids='validate_params')['spark_conn_id'] }}",
        conf={"spark.master": "{{ ti.xcom_pull(task_ids='validate_params')['spark_master'] }}"},
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='download_mondo_terms') }}",
            "--output", "{{ ti.xcom_pull(task_ids='validate_params')['output_path'] }}",
        ],
        name="normalize-mondo-terms",
        execution_timeout=timedelta(hours=2),
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------------------------------------------------------------
    # Task: Index Mondo Terms (Spark)
    # ---------------------------------------------------------------------
    index_mondo_terms = SparkSubmitOperator(
        task_id="index_mondo_terms",
        application="/opt/airflow/dags/spark/index_mondo.py",  # placeholder path
        conn_id="{{ ti.xcom_pull(task_ids='validate_params')['spark_conn_id'] }}",
        conf={"spark.master": "{{ ti.xcom_pull(task_ids='validate_params')['spark_master'] }}"},
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='validate_params')['output_path'] }}",
            "--output", "{{ ti.xcom_pull(task_ids='validate_params')['index_path'] }}",
        ],
        name="index-mondo-terms",
        execution_timeout=timedelta(hours=2),
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------------------------------------------------------------
    # Task: Publish Mondo Dataset
    # ---------------------------------------------------------------------
    @task(task_id="publish_mondo", retries=0)
    def publish_mondo(params: dict) -> None:
        """
        Publishes the processed Mondo dataset to the target destination.
        This is a placeholder implementation; replace with actual publishing logic.
        """
        try:
            # Example: move files to a data lake or upload to S3
            output_path = params["output_path"]
            index_path = params["index_path"]
            # Placeholder logic
            logging.info("Publishing dataset: %s and index: %s", output_path, index_path)
            # Insert real publishing code here
        except Exception as exc:
            logging.error("Publishing failed: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Send Slack Notification
    # ---------------------------------------------------------------------
    @task(task_id="slack_notification", retries=0, trigger_rule=TriggerRule.ALL_SUCCESS)
    def slack_notification(params: dict) -> None:
        """
        Sends a Slack notification indicating pipeline completion.
        """
        webhook_url = params.get("slack_webhook")
        if not webhook_url:
            logging.warning("Slack webhook URL not configured; skipping notification.")
            return

        message = {
            "text": ":white_check_mark: *ETL Import Mondo* pipeline completed successfully."
        }
        try:
            response = requests.post(
                webhook_url,
                data=json.dumps(message),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            logging.info("Slack notification sent.")
        except Exception as exc:
            logging.error("Failed to send Slack notification: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Define task pipeline (dependencies)
    # ---------------------------------------------------------------------
    params = validate_params()
    downloaded_file = download_mondo_terms(params)

    # Set explicit dependencies using >> operator
    params >> downloaded_file
    downloaded_file >> normalize_mondo_terms
    normalize_mondo_terms >> index_mondo_terms
    index_mondo_terms >> publish_mondo(params)
    publish_mondo(params) >> slack_notification(params)