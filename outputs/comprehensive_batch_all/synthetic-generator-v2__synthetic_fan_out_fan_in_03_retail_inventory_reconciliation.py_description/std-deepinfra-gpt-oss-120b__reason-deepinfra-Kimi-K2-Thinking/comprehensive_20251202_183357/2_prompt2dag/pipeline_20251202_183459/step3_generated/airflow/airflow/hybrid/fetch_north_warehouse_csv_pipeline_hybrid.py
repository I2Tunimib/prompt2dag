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
    dag_id="fetch_north_warehouse_csv_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["warehouse", "fanin"],
) as dag:

    # Fetch tasks
    fetch_east_warehouse_csv = DockerOperator(
        task_id="fetch_east_warehouse_csv",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    fetch_north_warehouse_csv = DockerOperator(
        task_id="fetch_north_warehouse_csv",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    fetch_south_warehouse_csv = DockerOperator(
        task_id="fetch_south_warehouse_csv",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    fetch_west_warehouse_csv = DockerOperator(
        task_id="fetch_west_warehouse_csv",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Normalize tasks
    normalize_east_skus = DockerOperator(
        task_id="normalize_east_skus",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    normalize_north_skus = DockerOperator(
        task_id="normalize_north_skus",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    normalize_south_skus = DockerOperator(
        task_id="normalize_south_skus",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    normalize_west_skus = DockerOperator(
        task_id="normalize_west_skus",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Reconcile and report tasks
    reconcile_all_inventories = DockerOperator(
        task_id="reconcile_all_inventories",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    generate_final_report = DockerOperator(
        task_id="generate_final_report",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Dependencies
    fetch_north_warehouse_csv >> normalize_north_skus
    fetch_south_warehouse_csv >> normalize_south_skus
    fetch_east_warehouse_csv >> normalize_east_skus
    fetch_west_warehouse_csv >> normalize_west_skus

    [
        normalize_north_skus,
        normalize_south_skus,
        normalize_east_skus,
        normalize_west_skus,
    ] >> reconcile_all_inventories

    reconcile_all_inventories >> generate_final_report