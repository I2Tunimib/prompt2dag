from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ftp_vendor_inventory_processor",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["ftp", "vendor", "inventory"],
) as dag:

    wait_for_ftp_file = DockerOperator(
        task_id='wait_for_ftp_file',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_vendor_file = DockerOperator(
        task_id='download_vendor_file',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_vendor_data = DockerOperator(
        task_id='cleanse_vendor_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    merge_with_internal_inventory = DockerOperator(
        task_id='merge_with_internal_inventory',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    wait_for_ftp_file >> download_vendor_file >> cleanse_vendor_data >> merge_with_internal_inventory