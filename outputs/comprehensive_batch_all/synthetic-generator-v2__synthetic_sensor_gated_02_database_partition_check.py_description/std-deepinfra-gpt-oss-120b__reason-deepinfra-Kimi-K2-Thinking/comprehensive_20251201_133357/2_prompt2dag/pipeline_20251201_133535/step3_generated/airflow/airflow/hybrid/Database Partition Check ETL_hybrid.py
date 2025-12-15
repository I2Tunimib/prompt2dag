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
    dag_id="database_partition_check_etl",
    description="Sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "database", "partition"],
) as dag:

    wait_partition = DockerOperator(
        task_id='wait_partition',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_incremental = DockerOperator(
        task_id='extract_incremental',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_orders = DockerOperator(
        task_id='transform_orders',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_orders = DockerOperator(
        task_id='load_orders',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    wait_partition >> extract_incremental >> transform_orders >> load_orders