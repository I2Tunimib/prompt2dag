import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Resolve the shared data directory (fallback to /tmp/data if not set)
DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
VOLUME_MAPPING = f"{DATA_DIR}:/app/data"

with DAG(
    dag_id="medical_facility_accessibility",
    default_args=default_args,
    description="Assess medical facility accessibility via geocoding and distance calculations",
    schedule_interval=None,
    catchup=False,
    tags=["medical", "accessibility"],
) as dag:

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
            "INPUT_FILE": "facilities.csv",
            "OUTPUT_FILE": "table_data_2.json",
        },
        network_mode="app_network",
        volumes=[VOLUME_MAPPING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    reconcile_geocode = DockerOperator(
        task_id="reconcile_geocode",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "PRIMARY_COLUMN": "address",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": os.getenv("HERE_API_TOKEN", "YOUR_HERE_API_TOKEN"),
            "INPUT_FILE": "table_data_2.json",
            "OUTPUT_FILE": "reconciled_table_2.json",
        },
        network_mode="app_network",
        volumes=[VOLUME_MAPPING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    distance_public_transport = DockerOperator(
        task_id="distance_public_transport",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "public_transport",
            "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
            "OUTPUT_COLUMN": "distance_to_pt",
            "INPUT_FILE": "reconciled_table_2.json",
            "OUTPUT_FILE": "distance_pt_2.json",
        },
        network_mode="app_network",
        volumes=[VOLUME_MAPPING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    distance_residential = DockerOperator(
        task_id="distance_residential",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "residential_areas",
            "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
            "OUTPUT_COLUMN": "distance_to_residential",
            "INPUT_FILE": "distance_pt_2.json",
            "OUTPUT_FILE": "column_extended_2.json",
        },
        network_mode="app_network",
        volumes=[VOLUME_MAPPING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    save_final = DockerOperator(
        task_id="save_final",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        command="",
        environment={
            "DATASET_ID": "2",
            "INPUT_FILE": "column_extended_2.json",
            "OUTPUT_FILE": "enriched_data_2.csv",
        },
        network_mode="app_network",
        volumes=[VOLUME_MAPPING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    # Define task dependencies
    load_and_modify >> reconcile_geocode >> distance_public_transport >> distance_residential >> save_final