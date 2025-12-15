from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_vendor_a_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fanout_fanin"],
) as dag:

    # Fan‑out dummy task
    extract_parallel = DummyOperator(task_id="extract_parallel")

    # Task definitions (provided)

    extract_vendor_c = DockerOperator(
        task_id='extract_vendor_c',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_shipment_data = DockerOperator(
        task_id='cleanse_shipment_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_shipment_to_inventory = DockerOperator(
        task_id='load_shipment_to_inventory',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_etl_summary_email = DockerOperator(
        task_id='send_etl_summary_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_vendor_a = DockerOperator(
        task_id='extract_vendor_a',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_vendor_b = DockerOperator(
        task_id='extract_vendor_b',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Dependencies (fanout → fanin)
    extract_parallel >> [extract_vendor_a, extract_vendor_b, extract_vendor_c]
    [extract_vendor_a, extract_vendor_b, extract_vendor_c] >> cleanse_shipment_data
    cleanse_shipment_data >> load_shipment_to_inventory
    load_shipment_to_inventory >> send_etl_summary_email