from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="test_dbx_aws_dag_reuse",
    description="Comprehensive pipeline that orchestrates Databricks notebook executions with conditional branching and reusable clusters. Manual trigger only.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:

    start_pipeline = DockerOperator(
        task_id='start_pipeline',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    execute_primary_notebook = DockerOperator(
        task_id='execute_primary_notebook',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    intermediate_dummy_1 = DockerOperator(
        task_id='intermediate_dummy_1',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    determine_branch_path = DockerOperator(
        task_id='determine_branch_path',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    execute_secondary_notebook = DockerOperator(
        task_id='execute_secondary_notebook',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    terminal_branch_dummy = DockerOperator(
        task_id='terminal_branch_dummy',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    intermediate_dummy_2 = DockerOperator(
        task_id='intermediate_dummy_2',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    pipeline_completion = DockerOperator(
        task_id='pipeline_completion',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies
    start_pipeline >> execute_primary_notebook
    execute_primary_notebook >> intermediate_dummy_1
    intermediate_dummy_1 >> determine_branch_path
    determine_branch_path >> [terminal_branch_dummy, execute_secondary_notebook]
    execute_secondary_notebook >> intermediate_dummy_2
    intermediate_dummy_2 >> pipeline_completion