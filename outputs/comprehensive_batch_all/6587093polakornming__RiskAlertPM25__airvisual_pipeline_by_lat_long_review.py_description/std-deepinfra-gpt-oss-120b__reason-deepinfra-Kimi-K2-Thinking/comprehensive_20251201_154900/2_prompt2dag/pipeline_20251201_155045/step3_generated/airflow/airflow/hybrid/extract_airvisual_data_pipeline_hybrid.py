from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="extract_airvisual_data_pipeline",
    description="No description provided.",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["airvisual", "docker"],
) as dag:

    extract_airvisual_data = DockerOperator(
        task_id="extract_airvisual_data",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    validate_airvisual_json = DockerOperator(
        task_id="validate_airvisual_json",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_airvisual_to_postgresql = DockerOperator(
        task_id="load_airvisual_to_postgresql",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Set sequential dependencies
    extract_airvisual_data >> validate_airvisual_json >> load_airvisual_to_postgresql