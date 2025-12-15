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
    dag_id="extract_transaction_csv_pipeline",
    description="Comprehensive Pipeline Description",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
    is_paused_upon_creation=True,
) as dag:

    extract_transaction_csv = DockerOperator(
        task_id='extract_transaction_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    branch_account_type_check = DockerOperator(
        task_id='branch_account_type_check',
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

    # Define dependencies (fanout_fanin pattern)
    extract_transaction_csv >> branch_account_type_check
    branch_account_type_check >> [generate_fatca_report, generate_irs_report]
    [generate_fatca_report, generate_irs_report] >> archive_regulatory_reports