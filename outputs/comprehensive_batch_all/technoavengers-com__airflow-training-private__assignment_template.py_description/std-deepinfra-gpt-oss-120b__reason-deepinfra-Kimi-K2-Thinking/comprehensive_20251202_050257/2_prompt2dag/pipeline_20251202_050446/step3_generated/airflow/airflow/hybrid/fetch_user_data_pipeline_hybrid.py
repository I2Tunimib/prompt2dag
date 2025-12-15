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
    dag_id="fetch_user_data_pipeline",
    description="No description provided.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    fetch_user_data = DockerOperator(
        task_id='fetch_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_user_data = DockerOperator(
        task_id='transform_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_user_table = DockerOperator(
        task_id='create_user_table',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    insert_user_data = DockerOperator(
        task_id='insert_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    fetch_user_data >> transform_user_data >> create_user_table >> insert_user_data