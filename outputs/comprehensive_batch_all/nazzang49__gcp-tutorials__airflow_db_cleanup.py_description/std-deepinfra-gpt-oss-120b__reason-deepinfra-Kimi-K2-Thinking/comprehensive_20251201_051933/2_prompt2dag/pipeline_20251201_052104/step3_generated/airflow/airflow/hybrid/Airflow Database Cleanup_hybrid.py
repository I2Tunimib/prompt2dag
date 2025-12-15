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
    dag_id="airflow_database_cleanup",
    default_args=default_args,
    description="Maintenance workflow that periodically cleans old metadata entries from Airflow's MetaStore database tables to prevent excessive data accumulation.",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["maintenance", "cleanup"],
) as dag:

    extract_cleanup_configuration = DockerOperator(
        task_id="extract_cleanup_configuration",
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

    extract_cleanup_configuration >> cleanup_airflow_metadb