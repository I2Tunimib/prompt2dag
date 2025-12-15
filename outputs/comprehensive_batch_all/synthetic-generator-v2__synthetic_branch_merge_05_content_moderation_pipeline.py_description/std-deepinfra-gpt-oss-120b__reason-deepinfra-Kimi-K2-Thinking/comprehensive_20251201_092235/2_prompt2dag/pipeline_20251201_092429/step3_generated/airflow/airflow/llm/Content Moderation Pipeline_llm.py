# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for Content Moderation Pipeline
Author: Auto-generated
Date: 2024-06-28
"""

import json
import logging
import random
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.utils.timezone import timezone
from airflow.hooks.base import BaseHook

# -------------------------------------------------------------------------
# Default arguments and DAG definition
# -------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["alerts@example.com"],  # fallback email; can be overridden by connection
}

with DAG(
    dag_id="content_moderation_pipeline",
    description="Scans user‑generated content for toxicity, branches based on a 0.7 threshold, and merges results for audit logging.",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1, tzinfo=timezone("UTC")),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["moderation", "fanout_fanin"],
    max_active_runs=1,
) as dag:
    # -------------------------------------------------------------------------
    # Helper utilities
    # -------------------------------------------------------------------------

    def get_connection_extra(conn_id: str) -> dict:
        """
        Retrieve the ``extra`` JSON from an Airflow connection.
        Returns an empty dict if the connection does not exist or has no extra.
        """
        try:
            conn = BaseHook.get_connection(conn_id)
            return json.loads(conn.extra) if conn.extra else {}
        except Exception as exc:
            logging.warning("Connection %s not found or invalid: %s", conn_id, exc)
            return {}

    def notify_failure(context):
        """
        Send an email notification on task failure using the ``email_smtp`` connection.
        """
        subject = f"Airflow alert: Task {context['task_instance'].task_id} failed"
        html_content = f"""
        <p>Dag: {context['dag'].dag_id}<br>
        Task: {context['task_instance'].task_id}<br>
        Execution Time: {context['execution_date']}<br>
        Log URL: {context['task_instance'].log_url}</p>
        """
        # Use the generic SMTP connection if defined, otherwise fallback to default
        smtp_conn = BaseHook.get_connection("email_smtp")
        send_email(
            to=DEFAULT_ARGS["email"],
            subject=subject,
            html_content=html_content,
            conn_id="email_smtp",
        )

    # -------------------------------------------------------------------------
    # Task definitions using TaskFlow API
    # -------------------------------------------------------------------------

    @task(retries=2, retry_delay=timedelta(minutes=5), on_failure_callback=notify_failure)
    def extract_user_content() -> pd.DataFrame:
        """
        Load user‑generated content from a local CSV file.
        The CSV location is defined in the ``local_csv_filesystem`` connection's ``extra`` field
        under the key ``path``.
        Returns a pandas DataFrame.
        """
        conn_extra = get_connection_extra("local_csv_filesystem")
        csv_path = conn_extra.get("path", "/opt/airflow/data/user_content.csv")
        logging.info("Reading user content from %s", csv_path)

        try:
            df = pd.read_csv(csv_path)
            logging.info("Loaded %d rows of user content", len(df))
            return df
        except Exception as exc:
            logging.error("Failed to read CSV at %s: %s", csv_path, exc)
            raise AirflowFailException(f"Unable to load user content: {exc}")

    @task(retries=2, retry_delay=timedelta(minutes=5), on_failure_callback=notify_failure)
    def evaluate_toxicity(user_df: pd.DataFrame) -> dict:
        """
        Evaluate toxicity for each piece of content.
        A dummy toxicity score (0‑1) is generated for illustration.
        Content with a score >= 0.7 is considered toxic.
        Returns a dictionary with two keys:
            - ``safe``: list of safe content rows (as dicts)
            - ``toxic``: list of toxic content rows (as dicts)
        """
        safe_content = []
        toxic_content = []

        for _, row in user_df.iterrows():
            # Placeholder: replace with real model inference
            toxicity_score = random.random()
            row_dict = row.to_dict()
            row_dict["toxicity_score"] = toxicity_score

            if toxicity_score >= 0.7:
                toxic_content.append(row_dict)
            else:
                safe_content.append(row_dict)

        logging.info(
            "Toxicity evaluation completed: %d safe, %d toxic",
            len(safe_content),
            len(toxic_content),
        )
        return {"safe": safe_content, "toxic": toxic_content}

    @task(retries=2, retry_delay=timedelta(minutes=5), on_failure_callback=notify_failure)
    def publish_content(evaluation: dict):
        """
        Publish safe content via the ``publishing_api``.
        """
        safe_items = evaluation.get("safe", [])
        if not safe_items:
            logging.info("No safe content to publish.")
            return

        conn_extra = get_connection_extra("publishing_api")
        endpoint = conn_extra.get("endpoint", "http://publishing.api/publish")
        api_key = conn_extra.get("api_key", "")

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        for item in safe_items:
            payload = {"content_id": item.get("id"), "text": item.get("text")}
            try:
                response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                logging.info("Published content ID %s", payload["content_id"])
            except Exception as exc:
                logging.error("Failed to publish content ID %s: %s", payload["content_id"], exc)
                raise AirflowFailException(f"Publishing failed for ID {payload['content_id']}")

    @task(retries=2, retry_delay=timedelta(minutes=5), on_failure_callback=notify_failure)
    def remove_and_flag_content(evaluation: dict):
        """
        Remove toxic content and flag the associated users via the ``content_mgmt_api``.
        """
        toxic_items = evaluation.get("toxic", [])
        if not toxic_items:
            logging.info("No toxic content to process.")
            return

        conn_extra = get_connection_extra("content_mgmt_api")
        endpoint = conn_extra.get("endpoint", "http://content.api/moderate")
        api_key = conn_extra.get("api_key", "")

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        for item in toxic_items:
            payload = {
                "content_id": item.get("id"),
                "action": "remove_and_flag",
                "reason": "toxicity_score >= 0.7",
                "toxicity_score": item.get("toxicity_score"),
            }
            try:
                response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                logging.info("Removed & flagged content ID %s", payload["content_id"])
            except Exception as exc:
                logging.error(
                    "Failed to remove/flag content ID %s: %s", payload["content_id"], exc
                )
                raise AirflowFailException(f"Removal/flagging failed for ID {payload['content_id']}")

    @task(retries=2, retry_delay=timedelta(minutes=5), on_failure_callback=notify_failure)
    def audit_log(evaluation: dict):
        """
        Consolidate audit information from both safe and toxic processing steps
        and send it to the ``audit_logging_api``.
        """
        conn_extra = get_connection_extra("audit_logging_api")
        endpoint = conn_extra.get("endpoint", "http://audit.api/log")
        api_key = conn_extra.get("api_key", "")

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        audit_payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "safe_count": len(evaluation.get("safe", [])),
            "toxic_count": len(evaluation.get("toxic", [])),
            "details": {
                "safe": evaluation.get("safe", []),
                "toxic": evaluation.get("toxic", []),
            },
        }

        try:
            response = requests.post(
                endpoint, json=audit_payload, headers=headers, timeout=30
            )
            response.raise_for_status()
            logging.info("Audit log successfully sent. Response: %s", response.text)
        except Exception as exc:
            logging.error("Failed to send audit log: %s", exc)
            raise AirflowFailException(f"Audit logging failed: {exc}")

    # -------------------------------------------------------------------------
    # DAG wiring (fan‑out / fan‑in)
    # -------------------------------------------------------------------------

    user_df = extract_user_content()
    evaluation = evaluate_toxicity(user_df)

    # Fan‑out: safe and toxic branches run in parallel
    publish = publish_content(evaluation)
    remove_and_flag = remove_and_flag_content(evaluation)

    # Fan‑in: audit log runs after both branches complete
    audit = audit_log(evaluation)

    # Set explicit dependencies for clarity
    evaluation >> [publish, remove_and_flag] >> audit

# End of DAG definition.