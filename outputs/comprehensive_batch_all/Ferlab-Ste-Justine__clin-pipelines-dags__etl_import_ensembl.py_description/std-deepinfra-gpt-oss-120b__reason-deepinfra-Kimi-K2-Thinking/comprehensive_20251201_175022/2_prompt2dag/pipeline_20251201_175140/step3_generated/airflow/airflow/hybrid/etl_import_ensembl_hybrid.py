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
    dag_id="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "ensembl"],
) as dag:

    # Task definitions (as provided)

    extract_ensembl_files = DockerOperator(
        task_id='extract_ensembl_files',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_ensembl_mapping = DockerOperator(
        task_id='transform_ensembl_mapping',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Dependencies
    extract_ensembl_files >> transform_ensembl_mapping