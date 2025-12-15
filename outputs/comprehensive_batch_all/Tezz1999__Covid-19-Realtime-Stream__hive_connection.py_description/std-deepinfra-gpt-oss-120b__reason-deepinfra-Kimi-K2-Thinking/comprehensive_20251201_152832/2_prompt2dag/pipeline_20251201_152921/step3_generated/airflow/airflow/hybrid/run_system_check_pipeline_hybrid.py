from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="run_system_check_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="00 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pipeline"],
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

    run_system_check >> run_hive_script