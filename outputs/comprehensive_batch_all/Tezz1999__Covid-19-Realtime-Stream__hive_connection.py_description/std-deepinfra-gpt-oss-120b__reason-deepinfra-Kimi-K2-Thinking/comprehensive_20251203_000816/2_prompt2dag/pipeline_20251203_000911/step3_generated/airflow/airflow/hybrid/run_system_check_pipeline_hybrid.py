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
    dag_id="run_system_check_pipeline",
    description="Simple linear pipeline executing Hive operations for COVID-19 realtime streaming data.",
    schedule_interval="00 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hive", "docker", "sequential"],
) as dag:

    run_system_check = DockerOperator(
        task_id='run_system_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    execute_hive_script = DockerOperator(
        task_id='execute_hive_script',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    run_system_check >> execute_hive_script