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
    dag_id="download_noaa_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fanin"],
) as dag:

    download_bom = DockerOperator(
        task_id='download_bom',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_ecmwf = DockerOperator(
        task_id='download_ecmwf',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_jma = DockerOperator(
        task_id='download_jma',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_metoffice = DockerOperator(
        task_id='download_metoffice',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_noaa = DockerOperator(
        task_id='download_noaa',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_bom = DockerOperator(
        task_id='normalize_bom',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_ecmwf = DockerOperator(
        task_id='normalize_ecmwf',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_jma = DockerOperator(
        task_id='normalize_jma',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_metoffice = DockerOperator(
        task_id='normalize_metoffice',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_noaa = DockerOperator(
        task_id='normalize_noaa',
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

    # Set dependencies
    download_noaa >> normalize_noaa
    download_ecmwf >> normalize_ecmwf
    download_jma >> normalize_jma
    download_metoffice >> normalize_metoffice
    download_bom >> normalize_bom

    normalize_noaa >> merge_climate_data
    normalize_ecmwf >> merge_climate_data
    normalize_jma >> merge_climate_data
    normalize_metoffice >> merge_climate_data
    normalize_bom >> merge_climate_data