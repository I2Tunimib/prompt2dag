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


def parse_input_parameters(**kwargs):
    """
    Extract runtime parameters from ``dag_run.conf`` and the execution date.
    Returns a dictionary that is stored in XCom for downstream tasks.
    """
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    execution_date = kwargs["execution_date"]

    description = conf.get("description", "No description provided")
    git_commitish = conf.get("git_commitish", "main")

    compilation_config = {
        "logical_date": execution_date.isoformat(),
        "description": description,
        "git_commitish": git_commitish,
    }
    return compilation_config


def create_compilation_result(**kwargs):
    """
    Simulate a Dataform compilation step.
    Retrieves the compilation configuration from XCom and returns a mock
    compilation result identifier.
    """
    ti = kwargs["ti"]
    compilation_config = ti.xcom_pull(task_ids="parse_input_parameters")

    # In a real implementation you would call the Dataform API here.
    # For demonstration we generate a deterministic identifier.
    compilation_result_id = f"comp_{int(datetime.utcnow().timestamp())}"

    return {
        "compilation_result_id": compilation_result_id,
        "config": compilation_config,
    }


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
}

with DAG(
    dag_id="dataform_transformation_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataform", "transformation"],
) as dag:
    start = DummyOperator(task_id="start")

    parse_input = PythonOperator(
        task_id="parse_input_parameters",
        python_callable=parse_input_parameters,
        provide_context=True,
    )

    compile = PythonOperator(
        task_id="create_compilation_result",
        python_callable=create_compilation_result,
        provide_context=True,
    )

    invoke_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id="{{ var.value.dataform_project_id }}",
        location="{{ var.value.dataform_location }}",
        repository_id="{{ var.value.dataform_repository_id }}",
        compilation_result_id="{{ task_instance.xcom_pull(task_ids='create_compilation_result')['compilation_result_id'] }}",
        gcp_conn_id="modelling_cloud_default",
        asynchronous=True,
    )

    monitor_workflow = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id="{{ var.value.dataform_project_id }}",
        location="{{ var.value.dataform_location }}",
        workflow_invocation_id="{{ task_instance.xcom_pull(task_ids='create_workflow_invocation')['workflow_invocation_id'] }}",
        gcp_conn_id="modelling_cloud_default",
        poke_interval=60,
        timeout=60 * 60,
        allowed_states=["SUCCEEDED", "FAILED"],
    )

    end = DummyOperator(task_id="end")

    start >> parse_input >> compile >> invoke_workflow >> monitor_workflow >> end