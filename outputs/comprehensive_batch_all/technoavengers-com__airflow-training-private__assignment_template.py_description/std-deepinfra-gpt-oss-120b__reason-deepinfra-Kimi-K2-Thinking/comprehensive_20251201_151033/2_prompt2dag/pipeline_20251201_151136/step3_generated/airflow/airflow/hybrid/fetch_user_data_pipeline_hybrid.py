from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_user_data_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Task: fetch_user_data
    fetch_user_data = DockerOperator(
        task_id='fetch_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: transform_user_data
    transform_user_data = DockerOperator(
        task_id='transform_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: create_user_table
    create_user_table = DockerOperator(
        task_id='create_user_table',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: insert_user_data
    insert_user_data = DockerOperator(
        task_id='insert_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    fetch_user_data >> transform_user_data >> create_user_table >> insert_user_data