"""Airflow DAG for L2 load, segmentation processing, and SAP integration."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.timezone import timezone
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


def get_load_id(**context):
    """Generate a unique load identifier and push it to XCom."""
    load_id = f"load_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    context["ti"].xcom_push(key="load_id", value=load_id)
    return load_id


def workflow_registration(**context):
    """Register the workflow session in metadata tables."""
    load_id = context["ti"].xcom_pull(key="load_id")
    # Placeholder for actual registration logic, e.g., md_dwh.register_workflow(load_id)
    return f"registered_{load_id}"


def send_flg_to_sap(**context):
    """Send completion flag to SAP with row count from segmentation results."""
    # In a real implementation, retrieve row count from previous task or DB.
    row_count = 12345  # placeholder value
    # Placeholder for HTTP call to SAP system.
    # e.g., requests.post(sap_url, json={"load_id": load_id, "rows": row_count})
    return f"SAP notified with {row_count} rows"


def end_workflow(**context):
    """Mark the workflow session as successfully completed."""
    load_id = context["ti"].xcom_pull(key="load_id")
    # Placeholder for metadata update, e.g., md_dwh.mark_success(load_id)
    return f"workflow {load_id} completed"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

local_tz = timezone("Asia/Tashkent")

with DAG(
    dag_id="l2_load_segmentation_sap_integration",
    default_args=default_args,
    description="Load datasets from DWH L2, run segmentation, and notify SAP.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=20,
    tags=["dwh", "segmentation", "sap"],
) as dag:

    wait_for_l2_full_load = SqlSensor(
        task_id="wait_for_l2_full_load",
        conn_id="postgres_default",
        sql=(
            "SELECT flag FROM metadata.table "
            "WHERE process='l1_to_l2' AND status='SUCCESS';"
        ),
        poke_interval=60,
        timeout=60 * 60,  # 1 hour
    )

    get_load_id_task = PythonOperator(
        task_id="get_load_id",
        python_callable=get_load_id,
    )

    workflow_registration_task = PythonOperator(
        task_id="workflow_registration",
        python_callable=workflow_registration,
    )

    wait_for_success_end = ExternalTaskSensor(
        task_id="wait_for_success_end",
        external_dag_id="l2_load_segmentation_sap_integration",
        external_task_id=None,
        execution_delta=timedelta(days=1),
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        poke_interval=60,
        timeout=60 * 60 * 24,  # 24 hours
    )

    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id="run_sys_kill_all_session_pg",
        trigger_dag_id="sys_kill_all_session_pg",
        wait_for_completion=False,
    )

    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id="run_wf_data_preparation_for_reports",
        trigger_dag_id="wf_data_preparation_for_reports",
        wait_for_completion=False,
    )

    with TaskGroup("segmentation_group") as segmentation_group:
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

    end_task = PythonOperator(
        task_id="end",
        python_callable=end_workflow,
    )

    final = EmptyOperator(
        task_id="final",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    email_on_failure = EmailOperator(
        task_id="email_on_failure",
        to=["data-team@example.com"],
        subject="DAG {{ dag.dag_id }} failed",
        html_content=(
            "The DAG <strong>{{ dag.dag_id }}</strong> failed on run "
            "<strong>{{ run_id }}</strong>."
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Define workflow dependencies
    wait_for_l2_full_load >> get_load_id_task >> workflow_registration_task
    workflow_registration_task >> wait_for_success_end >> run_sys_kill_all_session_pg

    run_sys_kill_all_session_pg >> [
        run_wf_data_preparation_for_reports,
        segmentation_group,
    ]

    run_wf_data_preparation_for_reports >> end_task
    segmentation_group >> end_task

    end_task >> final

    # Ensure final runs after all tasks regardless of their state
    final.set_upstream(
        [
            wait_for_l2_full_load,
            get_load_id_task,
            workflow_registration_task,
            wait_for_success_end,
            run_sys_kill_all_session_pg,
            run_wf_data_preparation_for_reports,
            load_ds_client_segmentation,
            send_flg_to_sap_task,
            end_task,
        ]
    )

    final >> email_on_failure

    # Main linear flow
    wait_for_l2_full_load >> get_load_id_task
    get_load_id_task >> workflow_registration_task
    workflow_registration_task >> wait_for_success_end
    wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group]
    [run_wf_data_preparation_for_reports, segmentation_group] >> end_task
    end_task >> final
    final >> email_on_failure