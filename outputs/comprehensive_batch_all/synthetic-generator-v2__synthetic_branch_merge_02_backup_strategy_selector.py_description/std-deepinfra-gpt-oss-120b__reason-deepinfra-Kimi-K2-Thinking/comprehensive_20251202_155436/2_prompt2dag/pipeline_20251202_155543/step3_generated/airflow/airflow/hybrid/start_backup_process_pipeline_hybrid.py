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
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["backup", "fanout_fanin"],
) as dag:

    start_backup_process = DockerOperator(
        task_id='start_backup_process',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    date_check_task = DockerOperator(
        task_id='date_check_task',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    full_backup_task = DockerOperator(
        task_id='full_backup_task',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    incremental_backup_task = DockerOperator(
        task_id='incremental_backup_task',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    verify_backup_task = DockerOperator(
        task_id='verify_backup_task',
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

    # Define dependencies (fanout_fanin pattern)
    start_backup_process >> date_check_task
    date_check_task >> [full_backup_task, incremental_backup_task]
    [full_backup_task, incremental_backup_task] >> verify_backup_task
    verify_backup_task >> backup_complete