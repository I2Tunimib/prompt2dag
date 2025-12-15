"""Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration."""

from datetime import datetime, timedelta

import pendulum
import uuid

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

LOCAL_TZ = pendulum.timezone("Asia/Tashkent")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_load_id(**context):
    """Generate a unique load identifier and push it to XCom."""
    load_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="load_id", value=load_id)
    return load_id


def workflow_registration(**context):
    """Register the workflow session in metadata tables."""
    load_id = context["ti"].xcom_pull(key="load_id", task_ids="get_load_id")
    # Placeholder for actual registration logic (e.g., DB insert)
    # Example: md_dwh.register_workflow(load_id)
    return f"registered_{load_id}"


def send_flg_to_sap(**context):
    """Send completion flag to SAP system with row count from segmentation."""
    row_count = context["ti"].xcom_pull(
        key="row_count", task_ids="segmentation_group.load_ds_client_segmentation"
    )
    # Placeholder for SAP HTTP call
    # Example: sap_client.send_flag(load_id, row_count)
    return f"sent SAP flag with {row_count or 0} rows"


def mark_workflow_success(**context):
    """Mark the workflow session as successfully completed."""
    load_id = context["ti"].xcom_pull(key="load_id", task_ids="get_load_id")
    # Placeholder for metadata update
    # Example: md_dwh.mark_success(load_id)
    return f"workflow {load_id} marked success"


with DAG(
    dag_id="l2_dataset_load_with_segmentation",
    description="Load datasets from DWH L2 to L2, run segmentation and notify SAP.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=20,
    default_args=default_args,
    tags=["dwh", "segmentation", "sap"],
) as dag:
    # 1. Wait for L1 → L2 load completion
    wait_for_l2_full_load = SqlSensor(
        task_id="wait_for_l2_full_load",
        conn_id="postgres_default",
        sql="SELECT flag FROM metadata.load_status WHERE source='L1' AND target='L2' AND status='SUCCESS';",
        poke_interval=60,
        timeout=6 * 60 * 60,
        mode="reschedule",
    )

    # 2. Retrieve unique load identifier
    get_load_id_task = PythonOperator(
        task_id="get_load_id",
        python_callable=get_load_id,
    )

    # 3. Register workflow session
    workflow_registration_task = PythonOperator(
        task_id="workflow_registration",
        python_callable=workflow_registration,
    )

    # 4. Wait for previous day's DAG run to finish successfully
    wait_for_success_end = ExternalTaskSensor(
        task_id="wait_for_success_end",
        external_dag_id="l2_dataset_load_with_segmentation",
        execution_date_fn=lambda _: datetime.now(tz=LOCAL_TZ) - timedelta(days=1),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=12 * 60 * 60,
        mode="reschedule",
    )

    # 5. Trigger system utility DAG to kill all PostgreSQL sessions
    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id="run_sys_kill_all_session_pg",
        trigger_dag_id="sys_kill_all_session_pg",
        wait_for_completion=False,
    )

    # 6a. Trigger data preparation workflow for reports
    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id="run_wf_data_preparation_for_reports",
        trigger_dag_id="wf_data_preparation_for_reports",
        wait_for_completion=False,
    )

    # 6b. Segmentation group
    with TaskGroup(group_id="segmentation_group") as segmentation_group:
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id="load_ds_client_segmentation",
            trigger_dag_id="l1_to_l2_p_load_data_ds_client_segmentation_full",
            wait_for_completion=False,
        )

        send_flg_to_sap_task = PythonOperator(
            task_id="send_flg_to_sap",
            python_callable=send_flg_to_sap,
        )

        load_ds_client_segmentation >> send_flg_to_sap_task

    # 7. Mark workflow as successful
    end = PythonOperator(
        task_id="end",
        python_callable=mark_workflow_success,
    )

    # 8. Failure notification email
    email_on_failure = EmailOperator(
        task_id="email_on_failure",
        to=["data-eng@example.com"],
        subject="DAG Failure: {{ dag.dag_id }}",
        html_content="The DAG {{ dag.dag_id }} failed on {{ execution_date }}.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Define dependencies
    chain(
        wait_for_l2_full_load,
        get_load_id_task,
        workflow_registration_task,
        wait_for_success_end,
        run_sys_kill_all_session_pg,
        [run_wf_data_preparation_for_reports, segmentation_group],
        end,
    )

    # Failure email should fire if any upstream task fails
    [
        run_wf_data_preparation_for_reports,
        load_ds_client_segmentation,
        end,
    ] >> email_on_failure