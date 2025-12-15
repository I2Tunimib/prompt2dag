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
    dag_id="data_quality_gate",
    description="Implements a data quality gate for customer CSV data that ingests raw data, performs quality assessment, and conditionally routes to production or quarantine based on quality scores.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["data_quality"],
    is_paused_upon_creation=True,
) as dag:

    ingest_csv = DockerOperator(
        task_id='ingest_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quality_check = DockerOperator(
        task_id='quality_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    production_load = DockerOperator(
        task_id='production_load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quarantine_and_alert = DockerOperator(
        task_id='quarantine_and_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_alert_email = DockerOperator(
        task_id='send_alert_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanup = DockerOperator(
        task_id='cleanup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (fanout-fanin pattern)
    ingest_csv >> quality_check
    quality_check >> production_load
    quality_check >> quarantine_and_alert
    quarantine_and_alert >> send_alert_email
    [production_load, send_alert_email] >> cleanup