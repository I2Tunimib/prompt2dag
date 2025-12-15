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
    dag_id="start_backup_process_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["backup"],
    is_paused_upon_creation=True,
) as dag:

    start_backup_process = DockerOperator(
        task_id='start_backup_process',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    determine_backup_strategy = DockerOperator(
        task_id='determine_backup_strategy',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    full_backup = DockerOperator(
        task_id='full_backup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    incremental_backup = DockerOperator(
        task_id='incremental_backup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    verify_backup = DockerOperator(
        task_id='verify_backup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    backup_complete = DockerOperator(
        task_id='backup_complete',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (fanout -> fanin pattern)
    start_backup_process >> determine_backup_strategy
    determine_backup_strategy >> [full_backup, incremental_backup]
    [full_backup, incremental_backup] >> verify_backup
    verify_backup >> backup_complete