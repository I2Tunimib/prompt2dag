from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_claims_pipeline",
    description="Healthcare claims processing ETL pipeline implementing a staged ETL pattern with parallel extraction, transformation, and parallel loading stages.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "etl", "fanout_fanin"],
) as dag:

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

    # Define dependencies (fanout -> transform -> fanin)
    [extract_claims, extract_providers] >> transform_join
    transform_join >> [load_warehouse, refresh_bi]