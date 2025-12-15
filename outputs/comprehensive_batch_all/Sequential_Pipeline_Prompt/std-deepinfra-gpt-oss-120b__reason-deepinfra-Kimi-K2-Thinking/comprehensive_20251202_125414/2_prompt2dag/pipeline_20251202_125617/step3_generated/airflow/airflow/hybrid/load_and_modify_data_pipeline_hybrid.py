from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="load_and_modify_data_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sequential"],
) as dag:

    load_and_modify_data = DockerOperator(
        task_id="load_and_modify_data",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        environment={
            "DATA_DIR": "/app/data",
            "DATASET_ID": "2",
            "DATE_COLUMN": "Fecha_id",
            "TABLE_NAMING": "JOT_{}",
        },
        network_mode="app_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    reconcile_city_names = DockerOperator(
        task_id="reconcile_city_names",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        environment={
            "RECONCILIATOR_ID": "geocodingHere",
            "PRIMARY_COLUMN": "City",
            "OPTIONAL_COLUMNS": "County,Country",
            "API_TOKEN": "<HERE_API_TOKEN>",
        },
        network_mode="app_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    enrich_with_openmeteo = DockerOperator(
        task_id="enrich_with_openmeteo",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        environment={
            "WEATHER_ATTRIBUTES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR": "-",
        },
        network_mode="app_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    extend_columns = DockerOperator(
        task_id="extend_columns",
        image="i2t-backendwithintertwino6-column-extension:latest",
        environment={
            "EXTENDER_ID": "reconciledColumnExt",
        },
        network_mode="app_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        environment={
            "OUTPUT_DIR": "/app/data",
        },
        network_mode="app_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Sequential dependencies
    load_and_modify_data >> reconcile_city_names >> enrich_with_openmeteo >> extend_columns >> save_final_data