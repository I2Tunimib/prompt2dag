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
    dag_id="docker_sequential_enrichment",
    description="Sequential Docker‑based data enrichment pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["docker", "data‑pipeline"],
) as dag:
    # Shared configuration
    data_dir = "/app/data"
    docker_network = "app_network"
    volume_binding = {data_dir: {"bind": "/app/data", "mode": "rw"}}

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=docker_network,
        volumes=volume_binding,
        environment={
            "DATA_DIR": data_dir,
            "DATASET_ID": "2",
            "DATE_COLUMN": "Fecha_id",
            "TABLE_PREFIX": "JOT_",
        },
        command="",
    )

    data_reconciliation = DockerOperator(
        task_id="data_reconciliation",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=docker_network,
        volumes=volume_binding,
        environment={
            "DATA_DIR": data_dir,
            "PRIMARY_COLUMN": "City",
            "OPTIONAL_COLUMNS": "County,Country",
            "RECONCILIATOR_ID": "geocodingHere",
            # Replace with a real token or Airflow Variable as needed
            "HERE_API_TOKEN": "YOUR_HERE_API_TOKEN",
        },
        command="",
    )

    openmeteo_extension = DockerOperator(
        task_id="openmeteo_extension",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=docker_network,
        volumes=volume_binding,
        environment={
            "DATA_DIR": data_dir,
            "WEATHER_ATTRIBUTES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR": "-",
        },
        command="",
    )

    column_extension = DockerOperator(
        task_id="column_extension",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=docker_network,
        volumes=volume_binding,
        environment={
            "DATA_DIR": data_dir,
            "EXTENDER_ID": "reconciledColumnExt",
        },
        command="",
    )

    save_final = DockerOperator(
        task_id="save_final",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=docker_network,
        volumes=volume_binding,
        environment={"DATA_DIR": data_dir},
        command="",
    )

    # Define sequential dependencies
    load_and_modify >> data_reconciliation >> openmeteo_extension >> column_extension >> save_final