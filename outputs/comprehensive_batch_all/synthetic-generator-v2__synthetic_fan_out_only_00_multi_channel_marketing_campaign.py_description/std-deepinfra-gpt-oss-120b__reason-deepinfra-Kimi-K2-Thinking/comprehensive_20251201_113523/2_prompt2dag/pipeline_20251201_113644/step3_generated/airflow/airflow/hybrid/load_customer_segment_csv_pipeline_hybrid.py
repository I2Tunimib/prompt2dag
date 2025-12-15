from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="load_customer_segment_csv_pipeline",
    description="Comprehensive Pipeline Description",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fanout"],
) as dag:

    load_customer_segment_csv = DockerOperator(
        task_id='load_customer_segment_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_email_campaign = DockerOperator(
        task_id='send_email_campaign',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_push_notification = DockerOperator(
        task_id='send_push_notification',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_sms_campaign = DockerOperator(
        task_id='send_sms_campaign',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Fan‑out dependencies
    load_customer_segment_csv >> [send_email_campaign, send_sms_campaign, send_push_notification]