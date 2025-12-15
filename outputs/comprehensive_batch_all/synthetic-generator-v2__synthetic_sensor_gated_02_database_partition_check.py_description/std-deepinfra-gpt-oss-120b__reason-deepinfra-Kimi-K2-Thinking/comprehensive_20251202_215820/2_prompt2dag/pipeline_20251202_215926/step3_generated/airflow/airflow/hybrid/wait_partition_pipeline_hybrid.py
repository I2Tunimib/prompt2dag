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
    dag_id="wait_partition_pipeline",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
    tags=["sequential"],
) as dag:

    # Task: wait_partition
    wait_partition = DockerOperator(
        task_id='wait_partition',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: extract_incremental
    extract_incremental = DockerOperator(
        task_id='extract_incremental',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: transform
    transform = DockerOperator(
        task_id='transform',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: load
    load = DockerOperator(
        task_id='load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set dependencies (sequential pattern)
    wait_partition >> extract_incremental >> transform >> load