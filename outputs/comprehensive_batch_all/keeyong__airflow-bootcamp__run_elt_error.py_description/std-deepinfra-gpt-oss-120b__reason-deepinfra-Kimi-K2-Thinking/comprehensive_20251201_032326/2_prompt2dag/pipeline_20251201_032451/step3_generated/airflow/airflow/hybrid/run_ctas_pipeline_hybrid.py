from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="run_ctas_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["example"],
) as dag:

    run_ctas = DockerOperator(
        task_id='run_ctas',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    slack_failure_alert = DockerOperator(
        task_id='slack_failure_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    run_ctas >> slack_failure_alert