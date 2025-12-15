from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="etl_import_mondo",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "mondo"],
) as dag:

    validate_params = DockerOperator(
        task_id='validate_params',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_mondo_terms = DockerOperator(
        task_id='download_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_mondo_terms = DockerOperator(
        task_id='normalize_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    index_mondo_terms = DockerOperator(
        task_id='index_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    publish_mondo = DockerOperator(
        task_id='publish_mondo',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    notify_slack = DockerOperator(
        task_id='notify_slack',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    validate_params >> download_mondo_terms >> normalize_mondo_terms >> index_mondo_terms >> publish_mondo >> notify_slack