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
    dag_id="check_pcd_sftp_folder_pipeline",
    description="Staged ETL pipeline for Primary Care Data (PCD) processing with folder validation, parallel API extraction, Kubernetes‑based processing and email notification.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pcd", "etl"],
    is_paused_upon_creation=True,
) as dag:

    # Task: check_pcd_sftp_folder
    check_pcd_sftp_folder = DockerOperator(
        task_id='check_pcd_sftp_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: check_pcd_shared_folder
    check_pcd_shared_folder = DockerOperator(
        task_id='check_pcd_shared_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: extract_pcd_api
    extract_pcd_api = DockerOperator(
        task_id='extract_pcd_api',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: process_and_load_pcd_data
    process_and_load_pcd_data = DockerOperator(
        task_id='process_and_load_pcd_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: send_etl_notification
    send_etl_notification = DockerOperator(
        task_id='send_etl_notification',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder >> extract_pcd_api >> process_and_load_pcd_data >> send_etl_notification