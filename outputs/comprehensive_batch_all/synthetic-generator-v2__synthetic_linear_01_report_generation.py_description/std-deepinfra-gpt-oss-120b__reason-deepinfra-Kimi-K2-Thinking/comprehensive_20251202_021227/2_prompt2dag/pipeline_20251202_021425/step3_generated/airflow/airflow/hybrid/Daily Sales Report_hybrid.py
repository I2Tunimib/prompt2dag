from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_sales_report",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL, converting to CSV, creating a PDF chart, and emailing the report.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["sales", "report"],
) as dag:

    query_sales_data = DockerOperator(
        task_id='query_sales_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_to_csv = DockerOperator(
        task_id='transform_to_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_pdf_chart = DockerOperator(
        task_id='generate_pdf_chart',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    email_sales_report = DockerOperator(
        task_id='email_sales_report',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    query_sales_data >> transform_to_csv >> generate_pdf_chart >> email_sales_report