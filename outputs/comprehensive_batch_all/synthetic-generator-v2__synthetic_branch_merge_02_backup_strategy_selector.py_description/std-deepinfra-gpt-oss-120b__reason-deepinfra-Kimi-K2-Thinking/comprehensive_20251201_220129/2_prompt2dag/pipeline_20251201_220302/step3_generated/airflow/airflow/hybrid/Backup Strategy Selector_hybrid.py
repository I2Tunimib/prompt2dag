from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="backup_strategy_selector",
    description="Comprehensive pipeline that selects a backup strategy (full or incremental) based on the day of week, using a branch‑merge pattern.",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["backup", "fanout_fanin"],
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

    verify_backup = DockerOperator(
        task_id="verify_backup",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    finalize_backup = DockerOperator(
        task_id="finalize_backup",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Define dependencies (fanout_fanin pattern)
    start_backup_process >> determine_backup_strategy
    determine_backup_strategy >> [perform_full_backup, perform_incremental_backup]
    [perform_full_backup, perform_incremental_backup] >> verify_backup
    verify_backup >> finalize_backup