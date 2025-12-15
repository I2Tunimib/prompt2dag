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
    dag_id="wait_partition_pipeline",
    description="Database Partition Check ETL",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
) as dag:

    wait_partition = DockerOperator(
        task_id="wait_partition",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    extract_incremental = DockerOperator(
        task_id="extract_incremental",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    transform_orders = DockerOperator(
        task_id="transform_orders",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_orders = DockerOperator(
        task_id="load_orders",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    wait_partition >> extract_incremental >> transform_orders >> load_orders