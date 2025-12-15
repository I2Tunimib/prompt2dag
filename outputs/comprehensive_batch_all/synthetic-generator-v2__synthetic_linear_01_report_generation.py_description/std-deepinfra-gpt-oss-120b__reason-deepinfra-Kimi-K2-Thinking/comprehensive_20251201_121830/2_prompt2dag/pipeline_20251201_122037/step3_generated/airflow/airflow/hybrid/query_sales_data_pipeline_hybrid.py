from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="query_sales_data_pipeline",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL sales data, converting it to CSV, creating a PDF chart, and emailing the report to management.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["sales", "reporting"],
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

    query_sales_data >> transform_to_csv >> generate_pdf_chart >> email_sales_report