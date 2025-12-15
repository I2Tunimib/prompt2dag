from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="simple_linear_data_pipeline",
    description="Simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data.",
    schedule_interval="00 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "hive", "docker"],
) as dag:

    # Task: run_system_check
    run_system_check = DockerOperator(
        task_id='run_system_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: run_hive_script
    run_hive_script = DockerOperator(
        task_id='run_hive_script',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set dependencies (sequential pattern)
    run_system_check >> run_hive_script