from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="healthcare_claims_etl",
    description="Comprehensive Pipeline Description",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "etl"],
) as dag:

    # ---- Task Definitions (provided) ----

    extract_claims = DockerOperator(
        task_id='extract_claims',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_providers = DockerOperator(
        task_id='extract_providers',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_join = DockerOperator(
        task_id='transform_join',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_warehouse = DockerOperator(
        task_id='load_warehouse',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    refresh_bi = DockerOperator(
        task_id='refresh_bi',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # ---- Dependencies (fanout_fanin pattern) ----
    [extract_claims, extract_providers] >> transform_join
    transform_join >> [load_warehouse, refresh_bi]