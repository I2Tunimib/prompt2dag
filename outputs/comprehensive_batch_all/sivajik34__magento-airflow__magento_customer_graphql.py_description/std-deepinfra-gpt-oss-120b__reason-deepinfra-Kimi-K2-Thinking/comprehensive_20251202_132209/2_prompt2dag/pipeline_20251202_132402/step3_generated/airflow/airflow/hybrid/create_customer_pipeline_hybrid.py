from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="create_customer_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # Task definitions
    create_customer = DockerOperator(
        task_id="create_customer",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    generate_customer_token = DockerOperator(
        task_id="generate_customer_token",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    get_customer_info = DockerOperator(
        task_id="get_customer_info",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Dependencies (sequential pattern)
    create_customer >> generate_customer_token >> get_customer_info