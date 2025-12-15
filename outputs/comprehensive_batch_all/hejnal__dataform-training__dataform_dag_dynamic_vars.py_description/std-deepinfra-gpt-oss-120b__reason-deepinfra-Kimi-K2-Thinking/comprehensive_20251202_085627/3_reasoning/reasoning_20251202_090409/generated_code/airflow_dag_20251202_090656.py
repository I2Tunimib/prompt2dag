"""
Dataform Transformation Pipeline DAG.

This linear pipeline orchestrates Google Cloud Dataform workflows for data processing.
It triggers upon completion of upstream data ingestion and executes six sequential tasks.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from google.cloud.dataform_v1beta1 import DataformWorkflowInvocationState

# Pipeline configuration
# In production, consider using Airflow Variables
_CONFIG = {
    "project_id": "your-gcp-project-id",
    "region": "us-central1",
    "repository_id": "your-dataform-repo",
    "upstream_dag_id": "dataform-training-data-ingestion",
    "gcp_conn_id": "modelling_cloud_default",
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
    "start_date": datetime(2024, 1, 1),
}

def parse_input_parameters(**context):
    """Parse runtime parameters and store compilation configuration in XCom."""
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    
    # Extract parameters with fallbacks
    git_commitish = conf.get("git_commitish", "main")
    
    # Prepare compilation configuration
    compilation_config = {
        "git_commitish": git_commitish,
        "code_compilation_config": {
            "default_database": _CONFIG["project_id"],
            "default_schema": "dataform",
            "assertion_schema": "dataform_assertions",
        }
    }
    
    # Push to XCom for downstream tasks
    ti = context["task_instance"]
    ti.xcom_push(key="compilation_config", value=compilation_config)
    
    return compilation_config

def create_compilation_result(**context):
    """Create Dataform compilation result and store its name in XCom."""
    from google.cloud import dataform_v1beta1
    
    ti = context["task_instance"]
    compilation_config = ti.xcom_pull(
        task_ids="parse_input_parameters", 
        key="compilation_config"
    )
    
    if not compilation_config:
        raise ValueError("No compilation configuration found in XCom")
    
    client = dataform_v1beta1.DataformClient()
    parent = (
        f"projects/{_CONFIG['project_id']}/locations/{_CONFIG['region']}"
        f"/repositories/{_CONFIG['repository_id']}"
    )
    
    compilation_result = dataform_v1beta1.CompilationResult(
        git_commitish=compilation_config["git_commitish"],
        code_compilation_config=dataform_v1beta1.CodeCompilationConfig(
            default_database=compilation_config["code_compilation_config"]["default_database"],
            default_schema=compilation_config["code_compilation_config"]["default_schema"],
            assertion_schema=compilation_config["code_compilation_config"]["assertion_schema"],
        )
    )
    
    request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=parent,
        compilation_result=compilation_result,
    )
    
    response = client.create_compilation_result(request=request)
    
    # Store compilation result name for downstream tasks
    ti.xcom_push(key="compilation_result_name", value=response.name)
    
    return response.name

with DAG(
    dag_id="dataform_transformation_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataform", "transformation", "gcp"],
) as dag:
    
    # Wait for upstream data ingestion DAG to complete
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_data_ingestion",
        external_dag_id=_CONFIG["upstream_dag_id"],
        external_task_id=None,  # Wait for entire DAG completion
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED, State.UPSTREAM_FAILED],
        timeout=3600,
        poke_interval=60,
        mode="poke",
    )
    
    start = DummyOperator(task_id="start")
    
    parse_params = PythonOperator(
        task_id="parse_input_parameters",
        python_callable=parse_input_parameters,
    )
    
    create_compilation = PythonOperator(
        task_id="create_compilation_result",
        python_callable=create_compilation_result,
    )
    
    # Create workflow invocation using the compilation result
    create_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=_CONFIG["project_id"],
        region=_CONFIG["region"],
        repository_id=_CONFIG["repository_id"],
        compilation_result="{{ task_instance.xcom_pull(task_ids='create_compilation_result', key='compilation_result_name') }}",
        gcp_conn_id=_CONFIG["gcp_conn_id"],
    )
    
    # Monitor workflow invocation state until completion
    monitor_workflow = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id=_CONFIG["project_id"],
        region=_CONFIG["region"],
        repository_id=_CONFIG["repository_id"],
        workflow_invocation_id="{{ task_instance.xcom_pull(task_ids='create_workflow_invocation', key='workflow_invocation_id') }}",
        expected_states={
            DataformWorkflowInvocationState.SUCCEEDED,
            DataformWorkflowInvocationState.FAILED,
        },
        gcp_conn_id=_CONFIG["gcp_conn_id"],
        poke_interval=60,
        timeout=3600,
    )
    
    end = DummyOperator(task_id="end")
    
    # Define linear workflow dependencies
    (
        wait_for_upstream
        >> start
        >> parse_params
        >> create_compilation
        >> create_workflow
        >> monitor_workflow
        >> end
    )