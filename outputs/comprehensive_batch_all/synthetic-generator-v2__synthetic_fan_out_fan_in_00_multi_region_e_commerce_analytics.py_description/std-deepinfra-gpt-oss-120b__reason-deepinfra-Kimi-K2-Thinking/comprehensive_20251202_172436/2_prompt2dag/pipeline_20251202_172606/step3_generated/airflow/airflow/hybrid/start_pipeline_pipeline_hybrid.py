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
    dag_id="start_pipeline_pipeline",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
    tags=["fanout_fanin"],
) as dag:

    start_pipeline = DockerOperator(
        task_id="start_pipeline",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_apac = DockerOperator(
        task_id="ingest_apac",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_eu = DockerOperator(
        task_id="ingest_eu",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_us_east = DockerOperator(
        task_id="ingest_us_east",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_us_west = DockerOperator(
        task_id="ingest_us_west",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    convert_currency_apac = DockerOperator(
        task_id="convert_currency_apac",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    convert_currency_eu = DockerOperator(
        task_id="convert_currency_eu",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    convert_currency_us_east = DockerOperator(
        task_id="convert_currency_us_east",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    convert_currency_us_west = DockerOperator(
        task_id="convert_currency_us_west",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    aggregate_global_revenue = DockerOperator(
        task_id="aggregate_global_revenue",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    end_pipeline = DockerOperator(
        task_id="end_pipeline",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Define dependencies (fanout and fanin)
    start_pipeline >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]

    ingest_us_east >> convert_currency_us_east
    ingest_us_west >> convert_currency_us_west
    ingest_eu >> convert_currency_eu
    ingest_apac >> convert_currency_apac

    [convert_currency_us_east, convert_currency_us_west, convert_currency_eu, convert_currency_apac] >> aggregate_global_revenue

    aggregate_global_revenue >> end_pipeline