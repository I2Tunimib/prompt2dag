from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="RunELT_Alert",
    description="Comprehensive Pipeline Description",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["elt", "alert"],
) as dag:

    # Task: create_analytics_table
    create_analytics_table = DockerOperator(
        task_id="create_analytics_table",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Task: slack_failure_notifier
    slack_failure_notifier = DockerOperator(
        task_id="slack_failure_notifier",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Dependencies
    create_analytics_table >> slack_failure_notifier