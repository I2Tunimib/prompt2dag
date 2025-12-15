from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="start_backup_process_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="No description provided.",
    tags=["backup", "fanout_fanin"],
    is_paused_upon_creation=True,
) as dag:

    start_backup_process = DockerOperator(
        task_id="start_backup_process",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    determine_backup_strategy = DockerOperator(
        task_id="determine_backup_strategy",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    perform_full_backup = DockerOperator(
        task_id="perform_full_backup",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    perform_incremental_backup = DockerOperator(
        task_id="perform_incremental_backup",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    validate_backup_integrity = DockerOperator(
        task_id="validate_backup_integrity",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    finalize_backup_workflow = DockerOperator(
        task_id="finalize_backup_workflow",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Define dependencies (fanout_fanin pattern)
    start_backup_process >> determine_backup_strategy
    determine_backup_strategy >> [perform_full_backup, perform_incremental_backup]
    [perform_full_backup, perform_incremental_backup] >> validate_backup_integrity
    validate_backup_integrity >> finalize_backup_workflow