from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="regulatory_report_router",
    description="Processes financial transaction data and routes it to regulatory systems (FATCA/IRS) with fan‑out/fan‑in pattern.",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["regulatory", "reporting"],
) as dag:

    extract_transaction_csv = DockerOperator(
        task_id='extract_transaction_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    determine_account_routing = DockerOperator(
        task_id='determine_account_routing',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_fatca_report = DockerOperator(
        task_id='generate_fatca_report',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_irs_report = DockerOperator(
        task_id='generate_irs_report',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    archive_regulatory_reports = DockerOperator(
        task_id='archive_regulatory_reports',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (fan‑out/fan‑in)
    extract_transaction_csv >> determine_account_routing
    determine_account_routing >> [generate_fatca_report, generate_irs_report]
    [generate_fatca_report, generate_irs_report] >> archive_regulatory_reports