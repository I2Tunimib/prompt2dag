"""Airflow DAG for a linear Dataform transformation pipeline.

The DAG parses runtime parameters, creates a Dataform compilation result,
triggers a workflow invocation, monitors its state, and completes execution.
It is triggered externally (schedule_interval=None) and does not perform
catch‑up runs.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from airflow.utils.dates import days_ago


def parse_input_parameters(**context):
    """Extract runtime parameters and push a compilation configuration to XCom."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    logical_date = context["logical_date"]
    description = conf.get("description", "No description")
    git_commitish = conf.get("git_commitish", "main")

    compilation_config = {
        "logical_date": logical_date.isoformat(),
        "description": description,
        "git_commitish": git_commitish,
    }

    context["ti"].xcom_push(key="compilation_config", value=compilation_config)


def create_compilation_result(**context):
    """Simulate Dataform compilation and push the result to XCom."""
    ti = context["ti"]
    config = ti.xcom_pull(
        key="compilation_config", task_ids="parse_input_parameters"
    )
    # In a real implementation, invoke the Dataform client here.
    compilation_result = {
        "result_id": "dummy_compilation_result",
        "config": config,
    }
    ti.xcom_push(key="compilation_result", value=compilation_result)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
}

with DAG(
    dag_id="dataform_transformation_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataform"],
) as dag:
    start = DummyOperator(task_id="start")

    parse_params = PythonOperator(
        task_id="parse_input_parameters",
        python_callable=parse_input_parameters,
    )

    compile_result = PythonOperator(
        task_id="create_compilation_result",
        python_callable=create_compilation_result,
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id="my-gcp-project",
        location="us-central1",
        repository_id="my-repo",
        compilation_result="{{ ti.xcom_pull(key='compilation_result', task_ids='create_compilation_result') }}",
        gcp_conn_id="modelling_cloud_default",
        asynchronous=True,
    )

    monitor_workflow = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id="my-gcp-project",
        location="us-central1",
        repository_id="my-repo",
        workflow_invocation_id="{{ ti.xcom_pull(key='workflow_invocation_id', task_ids='create_workflow_invocation') }}",
        gcp_conn_id="modelling_cloud_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        failed_states=["FAILED"],
        success_states=["SUCCEEDED"],
    )

    end = DummyOperator(task_id="end")

    # Define linear dependencies
    start >> parse_params >> compile_result >> create_workflow_invocation >> monitor_workflow >> end