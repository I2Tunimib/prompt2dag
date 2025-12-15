# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: etl_import_ensembl
Description: Comprehensive Pipeline Description
Pattern: fanout
Generation Time: 2024-06-30T12:00:00Z
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import get_current_context, task, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Default arguments applied to all tasks
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 300,  # 5 minutes
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def _send_slack_message(message: str) -> None:
    """
    Send a message to Slack using the ``slack_api`` connection.
    The connection must contain a ``webhook_url`` in its extra JSON.
    """
    ctx = get_current_context()
    slack_conn = ctx["ti"].get_dagrun().get_task_instance("notify_slack_start").get_conn_id()
    # Retrieve connection details via Airflow's connection system
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("slack_api")
    try:
        extra: Dict[str, Any] = json.loads(conn.extra or "{}")
        webhook_url = extra.get("webhook_url")
        if not webhook_url:
            raise AirflowException("Slack webhook URL not defined in connection extra.")
        response = requests.post(
            webhook_url,
            json={"text": message},
            timeout=10,
        )
        response.raise_for_status()
        logging.info("Slack notification sent successfully.")
    except Exception as exc:
        logging.error("Failed to send Slack notification: %s", exc)
        raise AirflowException(f"Slack notification failed: {exc}") from exc


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,  # Disabled schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "ensembl", "fanout"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    # ------------------------------------------------------------------
    # Task: notify_slack_start
    # ------------------------------------------------------------------
    @task(retries=1, retry_delay=300, task_id="notify_slack_start")
    def notify_slack_start() -> None:
        """Notify Slack that the DAG has started."""
        _send_slack_message(":rocket: *ETL Import Ensembl* DAG has started.")

    # ------------------------------------------------------------------
    # Task: check_and_download_ensembl_files
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=300, task_id="check_and_download_ensembl_files")
    def check_and_download_ensembl_files() -> None:
        """
        Check the Ensembl FTP server for mapping files and download them
        to the S3 data lake.
        """
        from airflow.hooks.base import BaseHook
        import boto3
        import ftplib
        import os
        import tempfile

        # Retrieve FTP connection
        ftp_conn = BaseHook.get_connection("ensembl_ftp")
        host = ftp_conn.host
        user = ftp_conn.login or "anonymous"
        password = ftp_conn.password or ""

        # Retrieve S3 connection
        s3_conn = BaseHook.get_connection("s3_data_lake")
        s3_bucket = s3_conn.schema  # bucket name stored in schema field
        s3 = boto3.client(
            "s3",
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            region_name=s3_conn.extra_dejson.get("region_name", "us-east-1"),
        )

        try:
            with ftplib.FTP(host) as ftp:
                ftp.login(user=user, passwd=password)
                ftp.cwd("/pub/current_mysql/ensembl_mart_*/")  # example path
                files = ftp.nlst()
                mapping_files = [f for f in files if f.endswith(".txt.gz")]

                if not mapping_files:
                    raise AirflowException("No mapping files found on Ensembl FTP.")

                for filename in mapping_files:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        logging.info("Downloading %s", filename)
                        ftp.retrbinary(f"RETR {filename}", tmp_file.write)
                        tmp_file_path = tmp_file.name

                    s3_key = f"ensembl/mapping/{filename}"
                    logging.info("Uploading %s to s3://%s/%s", filename, s3_bucket, s3_key)
                    s3.upload_file(tmp_file_path, s3_bucket, s3_key)
                    os.remove(tmp_file_path)

        except Exception as exc:
            logging.error("Error during Ensembl file download: %s", exc)
            raise AirflowException(f"Failed to download Ensembl files: {exc}") from exc

    # ------------------------------------------------------------------
    # Task: process_ensembl_mapping_spark
    # ------------------------------------------------------------------
    process_ensembl_mapping_spark = SparkSubmitOperator(
        task_id="process_ensembl_mapping_spark",
        application="/opt/airflow/dags/spark_jobs/process_ensembl_mapping.py",
        name="process_ensembl_mapping",
        conn_id="kubernetes_etl",
        conf={"spark.master": "k8s"},
        packages="org.apache.hadoop:hadoop-aws:3.3.2",
        executor_memory="2g",
        driver_memory="1g",
        num_executors=2,
        retries=3,
        retry_delay=300,
        execution_timeout=None,
        dag=dag,
    )

    # ------------------------------------------------------------------
    # Branching based on Spark job outcome
    # ------------------------------------------------------------------
    @task(task_id="outcome_branch")
    def outcome_branch(**context) -> str:
        """
        Branch to success or failure notification based on the state of
        ``process_ensembl_mapping_spark``.
        """
        ti = context["ti"]
        spark_task_state = ti.xcom_pull(
            task_ids="process_ensembl_mapping_spark", key="return_value"
        )
        # SparkSubmitOperator returns None on success; failures raise AirflowException.
        # We inspect the task instance state instead.
        spark_task_instance = ti.get_dagrun().get_task_instance("process_ensembl_mapping_spark")
        if spark_task_instance and spark_task_instance.state == "success":
            return "notify_slack_success"
        return "notify_slack_failure"

    # ------------------------------------------------------------------
    # Task: notify_slack_success
    # ------------------------------------------------------------------
    @task(retries=1, retry_delay=300, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, task_id="notify_slack_success")
    def notify_slack_success() -> None:
        """Notify Slack that the DAG completed successfully."""
        _send_slack_message(":white_check_mark: *ETL Import Ensembl* DAG completed successfully.")

    # ------------------------------------------------------------------
    # Task: notify_slack_failure
    # ------------------------------------------------------------------
    @task(retries=1, retry_delay=300, trigger_rule=TriggerRule.ONE_FAILED, task_id="notify_slack_failure")
    def notify_slack_failure() -> None:
        """Notify Slack that the DAG failed."""
        _send_slack_message(":x: *ETL Import Ensembl* DAG failed. Check Airflow logs for details.")

    # ------------------------------------------------------------------
    # Define task dependencies (fanout pattern)
    # ------------------------------------------------------------------
    start = notify_slack_start()
    download = check_and_download_ensembl_files()
    spark = process_ensembl_mapping_spark
    branch = outcome_branch()
    success = notify_slack_success()
    failure = notify_slack_failure()

    start >> download >> spark >> branch
    branch >> success
    branch >> failure

# End of DAG definition.