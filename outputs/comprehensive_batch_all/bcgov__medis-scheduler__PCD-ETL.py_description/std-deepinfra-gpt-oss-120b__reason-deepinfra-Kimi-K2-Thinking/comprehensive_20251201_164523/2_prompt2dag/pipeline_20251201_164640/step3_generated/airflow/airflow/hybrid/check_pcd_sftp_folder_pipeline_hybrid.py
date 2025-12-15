from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="check_pcd_sftp_folder_pipeline",
    default_args=default_args,
    description="Pipeline to check PCD folders, extract data, process and send notification.",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:

    # Entry point task
    check_pcd_sftp_folder = DockerOperator(
        task_id='check_pcd_sftp_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Subsequent folder check
    check_pcd_shared_folder = DockerOperator(
        task_id='check_pcd_shared_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Dummy tasks representing start points for extraction and processing
    start_pcd_extract_1 = DummyOperator(task_id='start_pcd_extract_1')
    start_pcd_extract_2 = DummyOperator(task_id='start_pcd_extract_2')

    # Extract data from PCD API
    extract_pcd_api = DockerOperator(
        task_id='extract_pcd_api',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Process and load the extracted data
    process_and_load_pcd = DockerOperator(
        task_id='process_and_load_pcd',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Send ETL completion notification
    send_etl_notification = DockerOperator(
        task_id='send_etl_notification',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (sequential pattern)
    check_pcd_sftp_folder >> check_pcd_shared_folder
    check_pcd_shared_folder >> start_pcd_extract_1
    start_pcd_extract_1 >> extract_pcd_api
    extract_pcd_api >> start_pcd_extract_2
    start_pcd_extract_2 >> process_and_load_pcd
    process_and_load_pcd >> send_etl_notification