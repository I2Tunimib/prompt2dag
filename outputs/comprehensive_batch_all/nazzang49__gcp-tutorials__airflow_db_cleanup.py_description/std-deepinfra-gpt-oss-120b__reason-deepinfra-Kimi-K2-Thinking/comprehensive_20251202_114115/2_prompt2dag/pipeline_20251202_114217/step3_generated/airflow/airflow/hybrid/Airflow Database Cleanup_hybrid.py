from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airflow_database_cleanup",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["cleanup"],
) as dag:

    load_cleanup_configuration = DockerOperator(
        task_id="load_cleanup_configuration",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    cleanup_airflow_metadb = DockerOperator(
        task_id="cleanup_airflow_metadb",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_cleanup_configuration >> cleanup_airflow_metadb