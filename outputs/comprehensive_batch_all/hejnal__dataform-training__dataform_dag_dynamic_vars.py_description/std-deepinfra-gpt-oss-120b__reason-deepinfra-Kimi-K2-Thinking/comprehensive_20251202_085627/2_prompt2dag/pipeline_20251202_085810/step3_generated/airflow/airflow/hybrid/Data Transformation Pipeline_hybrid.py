from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_transformation_pipeline",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["data_transformation"],
) as dag:

    initialize_pipeline = DockerOperator(
        task_id='initialize_pipeline',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    parse_input_params = DockerOperator(
        task_id='parse_input_params',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_compilation_result = DockerOperator(
        task_id='create_compilation_result',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_workflow_invocation = DockerOperator(
        task_id='create_workflow_invocation',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    monitor_workflow_state = DockerOperator(
        task_id='monitor_workflow_state',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    finalize_pipeline = DockerOperator(
        task_id='finalize_pipeline',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    initialize_pipeline >> parse_input_params >> create_compilation_result >> create_workflow_invocation >> monitor_workflow_state >> finalize_pipeline