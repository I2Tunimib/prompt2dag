from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="supply_chain_shipment_etl",
    description="Supply Chain Shipment ETL",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
    tags=["supply_chain"],
) as dag:
    # Placeholder for upstream extraction step
    join_extraction = DummyOperator(task_id="join_extraction")

    # Task: cleanse_and_normalize_shipments
    cleanse_and_normalize_shipments = DockerOperator(
        task_id="cleanse_and_normalize_shipments",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Task: load_shipments_to_inventory
    load_shipments_to_inventory = DockerOperator(
        task_id="load_shipments_to_inventory",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Task: send_etl_summary_email
    send_etl_summary_email = DockerOperator(
        task_id="send_etl_summary_email",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Define sequential dependencies
    join_extraction >> cleanse_and_normalize_shipments >> load_shipments_to_inventory >> send_etl_summary_email