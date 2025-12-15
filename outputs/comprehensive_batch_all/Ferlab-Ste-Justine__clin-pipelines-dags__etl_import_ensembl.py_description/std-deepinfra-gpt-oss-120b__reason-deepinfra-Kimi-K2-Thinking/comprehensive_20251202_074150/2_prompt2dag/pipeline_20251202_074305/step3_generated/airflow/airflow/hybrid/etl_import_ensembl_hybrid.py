from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import BranchPythonOperator


def choose_branch(**context):
    """
    Simple branching logic placeholder.
    In a real scenario, this could inspect the result of the previous task
    and decide whether to follow the success or failure path.
    """
    # For demonstration purposes, always follow the success path.
    return "notify_slack_success"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "ensembl", "fanout"],
) as dag:

    # Entry point: notify start
    notify_slack_start = DockerOperator(
        task_id="notify_slack_start",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Check and download Ensembl files
    check_and_download_ensembl_files = DockerOperator(
        task_id="check_and_download_ensembl_files",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Process Ensembl mapping with Spark
    process_ensembl_mapping_spark = DockerOperator(
        task_id="process_ensembl_mapping_spark",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Branching based on outcome
    outcome_branch = BranchPythonOperator(
        task_id="outcome_branch",
        python_callable=choose_branch,
        provide_context=True,
    )

    # Notify Slack on success
    notify_slack_success = DockerOperator(
        task_id="notify_slack_success",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Notify Slack on failure
    notify_slack_failure = DockerOperator(
        task_id="notify_slack_failure",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Define dependencies (fanout pattern)
    notify_slack_start >> check_and_download_ensembl_files >> process_ensembl_mapping_spark >> outcome_branch
    outcome_branch >> [notify_slack_success, notify_slack_failure]