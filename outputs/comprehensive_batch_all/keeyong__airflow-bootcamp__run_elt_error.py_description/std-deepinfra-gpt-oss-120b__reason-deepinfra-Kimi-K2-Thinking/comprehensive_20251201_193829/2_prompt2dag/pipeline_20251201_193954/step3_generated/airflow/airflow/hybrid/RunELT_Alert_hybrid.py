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
    dag_id="RunELT_Alert",
    description=(
        "Sequential ELT pipeline that builds analytics tables in Snowflake using CTAS "
        "operations, with data validation, atomic swaps, and Slack failure notifications."
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["elt", "snowflake", "slack"],
) as dag:

    # Task: run_ctas
    run_ctas = DockerOperator(
        task_id="run_ctas",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Task: notify_failure_slack
    notify_failure_slack = DockerOperator(
        task_id="notify_failure_slack",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # No explicit dependencies as per specification (both are entry points)