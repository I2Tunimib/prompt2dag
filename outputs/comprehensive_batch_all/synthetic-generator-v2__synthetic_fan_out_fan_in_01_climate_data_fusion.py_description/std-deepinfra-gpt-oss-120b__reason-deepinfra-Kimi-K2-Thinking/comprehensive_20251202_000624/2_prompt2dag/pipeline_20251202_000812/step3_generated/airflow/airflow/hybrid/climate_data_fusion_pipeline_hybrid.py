from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="climate_data_fusion_pipeline",
    description="No description provided.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["fanin"],
) as dag:

    download_agency_data = DockerOperator(
        task_id='download_agency_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_agency_data = DockerOperator(
        task_id='normalize_agency_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    merge_climate_data = DockerOperator(
        task_id='merge_climate_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (fanin pattern)
    download_agency_data >> normalize_agency_data >> merge_climate_data