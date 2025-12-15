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
    dag_id="wait_for_file_pipeline",
    description=(
        "Sensor‑gated pipeline that monitors daily transaction file arrivals, "
        "validates the file schema, and loads the data into a PostgreSQL table."
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:

    wait_for_file = DockerOperator(
        task_id="wait_for_file",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    validate_schema = DockerOperator(
        task_id="validate_schema",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_db = DockerOperator(
        task_id="load_db",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    wait_for_file >> validate_schema >> load_db