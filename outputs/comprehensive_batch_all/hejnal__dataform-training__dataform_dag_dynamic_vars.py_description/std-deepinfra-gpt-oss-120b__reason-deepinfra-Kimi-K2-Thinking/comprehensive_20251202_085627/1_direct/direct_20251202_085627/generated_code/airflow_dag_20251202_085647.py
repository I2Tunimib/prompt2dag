"""Dataform transformation pipeline DAG.

This DAG orchestrates a linear workflow that parses runtime parameters,
creates a Dataform compilation result, invokes a Dataform workflow, and
waits for its completion. It is triggered externally (e.g., by a dataset
completion) and does not perform backfill.
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


def parse_input_params(**context):
    """Extract runtime parameters and push compilation config to XCom."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    logical_date = context.get("logical_date")
    description = conf.get("description", "No description")
    git_commitish = conf.get("git_commitish", "main")

    compilation_config = {
        "logical_date": logical_date.isoformat() if logical_date else None,
        "description": description,
        "git_commitish": git_commitish,
    }
    return compilation_config


def create_compilation_result(**context):
    """Create a mock compilation result and push its ID to XCom."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="parse_input_parameters")
    # In a real implementation, call the Dataform API to compile here.
    compilation_result_id = f"comp_{int(datetime.utcnow().timestamp())}"
    return {
        "compilation_result_id": compilation_result_id,
        "config": config,
    }


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
}

with DAG(
    dag_id="dataform_transformation_pipeline",
    default_args=default_args,
    description="Linear Dataform transformation pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataform"],
) as dag:
    start = DummyOperator(task_id="start")

    parse_input = PythonOperator(
        task_id="parse_input_parameters",
        python_callable=parse_input_params,
        provide_context=True,
    )

    create_compilation = PythonOperator(
        task_id="create_compilation_result",
        python_callable=create_compilation_result,
        provide_context=True,
    )

    create_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id="my-gcp-project",
        location="us-central1",
        compilation_result_id=(
            "{{ ti.xcom_pull(task_ids='create_compilation_result')"
            "['compilation_result_id'] }}"
        ),
        # Let Dataform generate a workflow invocation ID.
        workflow_invocation_id=None,
        gcp_conn_id="modelling_cloud_default",
    )

    wait_workflow = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id="my-gcp-project",
        location="us-central1",
        workflow_invocation_id=(
            "{{ ti.xcom_pull(task_ids='create_workflow_invocation')"
            "['workflow_invocation_id'] }}"
        ),
        expected_states=["SUCCEEDED", "FAILED"],
        gcp_conn_id="modelling_cloud_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    end = DummyOperator(task_id="end")

    start >> parse_input >> create_compilation >> create_workflow >> wait_workflow >> end