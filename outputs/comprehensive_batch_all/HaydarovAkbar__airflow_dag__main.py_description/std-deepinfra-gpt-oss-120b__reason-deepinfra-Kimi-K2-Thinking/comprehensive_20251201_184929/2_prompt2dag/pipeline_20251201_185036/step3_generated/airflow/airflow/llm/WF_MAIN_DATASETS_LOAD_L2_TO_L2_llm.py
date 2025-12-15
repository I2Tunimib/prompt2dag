# Auto‑generated Airflow DAG
# DAG Name: WF_MAIN_DATASETS_LOAD_L2_TO_L2
# Generated on: 2024-06-28
# Description: Comprehensive Pipeline Description
# Pattern: fanout

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,                     # All tasks have 0 retries as per spec
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=2),
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="WF_MAIN_DATASETS_LOAD_L2_TO_L2",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,               # Disabled schedule
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fanout", "data-load"],
    max_active_runs=1,
    render_template_as_native_obj=True,
    timezone="UTC",
) as dag:

    # -----------------------------------------------------------------
    # Task: wait_for_l2_full_load
    # -----------------------------------------------------------------
    @task(task_id="wait_for_l2_full_load", retries=0)
    def wait_for_l2_full_load() -> None:
        """
        Wait for L2 Full Load Flag.
        Implementation should poll a flag in the data warehouse.
        """
        try:
            logging.info("Checking L2 full load flag...")
            # Placeholder logic – replace with real check using dwh_postgres connection
            flag_set = True
            if not flag_set:
                raise AirflowException("L2 full load flag not set.")
            logging.info("L2 full load flag detected.")
        except Exception as exc:
            logging.error("Error while waiting for L2 full load flag: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: get_load_id
    # -----------------------------------------------------------------
    @task(task_id="get_load_id", retries=0)
    def get_load_id() -> str:
        """
        Generate a unique load identifier.
        """
        try:
            load_id = str(uuid.uuid4())
            logging.info("Generated load_id: %s", load_id)
            return load_id
        except Exception as exc:
            logging.error("Failed to generate load_id: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: workflow_registration
    # -----------------------------------------------------------------
    @task(task_id="workflow_registration", retries=0)
    def workflow_registration(load_id: str) -> None:
        """
        Register workflow session in the tracking system.
        """
        try:
            logging.info("Registering workflow with load_id: %s", load_id)
            # Placeholder: insert registration logic using dwh_postgres
        except Exception as exc:
            logging.error("Workflow registration failed: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: wait_for_success_end
    # -----------------------------------------------------------------
    @task(task_id="wait_for_success_end", retries=0)
    def wait_for_success_end() -> None:
        """
        Wait for the previous day's workflow to complete successfully.
        """
        try:
            logging.info("Waiting for previous day workflow completion...")
            # Placeholder: implement actual waiting logic (e.g., sensor or DB check)
        except Exception as exc:
            logging.error("Error while waiting for previous workflow: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: run_sys_kill_all_session_pg
    # -----------------------------------------------------------------
    @task(task_id="run_sys_kill_all_session_pg", retries=0)
    def run_sys_kill_all_session_pg() -> None:
        """
        Trigger PostgreSQL session cleanup.
        """
        try:
            logging.info("Executing PostgreSQL session cleanup...")
            # Placeholder: execute cleanup using dwh_postgres connection
        except Exception as exc:
            logging.error("PostgreSQL session cleanup failed: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Parallel fan‑out tasks
    # -----------------------------------------------------------------
    @task(task_id="run_wf_data_preparation_for_reports", retries=0)
    def run_wf_data_preparation_for_reports() -> None:
        """
        Trigger reporting data preparation workflow.
        """
        try:
            logging.info("Starting data preparation for reports...")
            # Placeholder: trigger downstream workflow or script
        except Exception as exc:
            logging.error("Data preparation for reports failed: %s", exc)
            raise

    @task(task_id="load_ds_client_segmentation", retries=0)
    def load_ds_client_segmentation() -> None:
        """
        Trigger client segmentation load.
        """
        try:
            logging.info("Loading client segmentation dataset...")
            # Placeholder: load logic using dwh_postgres
        except Exception as exc:
            logging.error("Client segmentation load failed: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: send_flg_to_sap
    # -----------------------------------------------------------------
    @task(task_id="send_flg_to_sap", retries=0)
    def send_flg_to_sap() -> None:
        """
        Send completion flag to SAP system via HTTP API.
        """
        try:
            logging.info("Sending completion flag to SAP...")
            # Placeholder: use sap_http connection to POST flag
        except Exception as exc:
            logging.error("Failed to send flag to SAP: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Dummy task to join parallel branches
    # -----------------------------------------------------------------
    join_parallel = DummyOperator(
        task_id="join_parallel",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # -----------------------------------------------------------------
    # Task: end
    # -----------------------------------------------------------------
    @task(task_id="end", retries=0)
    def end() -> None:
        """
        Update workflow completion status.
        """
        try:
            logging.info("Marking workflow as completed.")
            # Placeholder: update status in tracking table
        except Exception as exc:
            logging.error("Failed to update workflow completion status: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Task: email_on_failure
    # -----------------------------------------------------------------
    @task(
        task_id="email_on_failure",
        retries=0,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def email_on_failure(context: dict) -> None:
        """
        Send failure notification email using the smtp_email connection.
        """
        try:
            from airflow.providers.smtp.hooks.smtp import SmtpHook

            dag_run = context.get("dag_run")
            task_instance = context.get("task_instance")
            subject = f"[Airflow] Failure in DAG {dag_run.dag_id}"
            body = (
                f"Task {task_instance.task_id} failed.\n"
                f"Execution date: {context.get('execution_date')}\n"
                f"Log URL: {task_instance.log_url}"
            )
            smtp = SmtpHook(smtp_conn_id="smtp_email")
            smtp.send_email(
                to=["data-team@example.com"],
                subject=subject,
                html_content=body,
            )
            logging.info("Failure notification email sent.")
        except Exception as exc:
            logging.error("Failed to send failure email: %s", exc)
            raise

    # -----------------------------------------------------------------
    # Define task dependencies (fan‑out pattern)
    # -----------------------------------------------------------------
    # Linear chain up to the fan‑out point
    wait_for_l2_full_load() >> get_load_id() >> workflow_registration() >> wait_for_success_end() >> run_sys_kill_all_session_pg()

    # Fan‑out after run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg() >> [run_wf_data_preparation_for_reports(), load_ds_client_segmentation()]

    # Join point
    [run_wf_data_preparation_for_reports(), load_ds_client_segmentation()] >> join_parallel

    # Subsequent tasks
    load_ds_client_segmentation() >> send_flg_to_sap()
    join_parallel >> end()
    end() >> email_on_failure()   # Executes only on failure due to trigger_rule

# End of DAG definition.