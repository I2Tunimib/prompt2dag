from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor


def parse_parameters(**context: Any) -> Dict[str, Any]:
    """
    Extract and process runtime parameters from dag_run configuration.
    
    Returns compilation configuration for Dataform.
    """
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    
    # Extract parameters with safe defaults
    project_id = conf.get("project_id", "your-gcp-project")
    region = conf.get("region", "us-central1")
    repository_id = conf.get("repository_id", "your-dataform-repo")
    git_commitish = conf.get("git_commitish", "main")
    
    # Use DAG run logical date
    logical_date = dag_run.logical_date.isoformat()
    
    # Optional description
    description = conf.get("description", f"Dataform compilation for {logical_date}")
    
    # Construct compilation configuration
    compilation_config = {
        "project_id": project_id,
        "region": region,
        "repository_id": repository_id,
        "compilation_result": {
            "git_commitish": git_commitish,
            "code_compilation_config": {
                "default_database": project_id,
                "default_schema": "dataform",
            },
        },
        "description": description,
    }
    
    return compilation_config


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,  # Zero retries as per requirements
    "retry_delay": timedelta(seconds=0),  # Immediate failure handling
}

# Define the upstream dataset that triggers this DAG
# Replace with actual URI of your dataform-training-data-ingestion dataset
upstream_dataset = Dataset("gs://your-bucket/dataform-training-data-ingestion")

with DAG(
    dag_id="dataform_transformation_pipeline",
    default_args=default_args,
    description="Linear Dataform transformation pipeline triggered by dataset completion",
    schedule=[upstream_dataset],  # Dataset-based scheduling
    catchup=False,  # Disabled as per requirements
    max_active_runs=1,  # Max parallel width of 1
    start_date=datetime(2024, 1, 1),
    tags=["dataform", "transformation", "gcp"],
) as dag:
    
    # Task 1: Start
    start = DummyOperator(
        task_id="start",
    )
    
    # Task 2: Parse Input Parameters
    parse_input_parameters = PythonOperator(
        task_id="parse_input_parameters",
        python_callable=parse_parameters,
    )
    
    # Task 3: Create Compilation Result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['project_id'] }}",
        region="{{ ti.xcom_pull(task_ids='parse_input_parameters')['region'] }}",
        repository_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['repository_id'] }}",
        compilation_result="{{ ti.xcom_pull(task_ids='parse_input_parameters')['compilation_result'] }}",
        gcp_conn_id="modelling_cloud_default",
    )
    
    # Task 4: Create Workflow Invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['project_id'] }}",
        region="{{ ti.xcom_pull(task_ids='parse_input_parameters')['region'] }}",
        repository_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['repository_id'] }}",
        workflow_invocation={
            "compilation_result": "{{ ti.xcom_pull(task_ids='create_compilation_result')['name'] }}",
        },
        gcp_conn_id="modelling_cloud_default",
    )
    
    # Task 5: Monitor Workflow Invocation State
    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['project_id'] }}",
        region="{{ ti.xcom_pull(task_ids='parse_input_parameters')['region'] }}",
        repository_id="{{ ti.xcom_pull(task_ids='parse_input_parameters')['repository_id'] }}",
        workflow_invocation_id="{{ ti.xcom_pull(task_ids='create_workflow_invocation')['name'].split('/')[-1] }}",
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour
        gcp_conn_id="modelling_cloud_default",
    )
    
    # Task 6: End
    end = DummyOperator(
        task_id="end",
    )
    
    # Define linear task dependencies
    start >> parse_input_parameters >> create_compilation_result >> create_workflow_invocation >> is_workflow_invocation_done >> end