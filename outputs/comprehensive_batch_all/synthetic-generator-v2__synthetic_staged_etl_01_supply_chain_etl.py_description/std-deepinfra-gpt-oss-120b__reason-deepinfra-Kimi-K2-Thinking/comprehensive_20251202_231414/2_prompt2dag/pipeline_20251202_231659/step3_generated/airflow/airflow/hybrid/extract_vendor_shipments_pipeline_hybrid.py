from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_vendor_shipments_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["pipeline"],
) as dag:

    # Placeholder for upstream task not defined in the provided snippets
    extract_join = DummyOperator(task_id="extract_join")

    extract_vendor_shipments = DockerOperator(
        task_id='extract_vendor_shipments',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_and_normalize_shipments = DockerOperator(
        task_id='cleanse_and_normalize_shipments',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_shipment_data = DockerOperator(
        task_id='load_shipment_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_summary_email = DockerOperator(
        task_id='send_summary_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    extract_join >> extract_vendor_shipments >> cleanse_and_normalize_shipments >> load_shipment_data >> send_summary_email