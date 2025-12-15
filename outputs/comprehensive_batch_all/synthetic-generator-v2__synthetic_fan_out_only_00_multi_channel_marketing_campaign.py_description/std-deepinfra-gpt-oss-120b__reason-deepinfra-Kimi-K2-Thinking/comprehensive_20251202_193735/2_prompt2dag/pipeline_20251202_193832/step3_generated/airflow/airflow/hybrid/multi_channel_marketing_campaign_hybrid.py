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
    dag_id="multi_channel_marketing_campaign",
    default_args=default_args,
    description="Executes a multi-channel marketing campaign by loading customer segment data and triggering parallel email, SMS, and push notification campaigns.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "fanout"],
) as dag:

    load_customer_segment_csv = DockerOperator(
        task_id="load_customer_segment_csv",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    send_email_campaign = DockerOperator(
        task_id="send_email_campaign",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    send_push_notification = DockerOperator(
        task_id="send_push_notification",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    send_sms_campaign = DockerOperator(
        task_id="send_sms_campaign",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_customer_segment_csv >> [send_email_campaign, send_sms_campaign, send_push_notification]