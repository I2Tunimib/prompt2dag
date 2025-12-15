import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

# Configuration from environment and Airflow Variables
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
DOCKER_DAEMON_URL = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
HERE_API_TOKEN = Variable.get("here_api_token", default_var="")

default_args = {
    "owner": "medical-facility-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medical_facility_accessibility_pipeline",
    description="Assesses medical facility accessibility by geocoding locations and calculating distances to infrastructure",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["medical", "geospatial", "accessibility"],
    default_args=default_args,
) as dag:

    load_and_modify_data = DockerOperator(
        task_id="load_and_modify_data",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
        },
        volumes=[f"{DATA_DIR}:/app/data"],
        network_mode="app_network",
        docker_url=DOCKER_DAEMON_URL,
    )

    reconciliation_geocoding = DockerOperator(
        task_id="reconciliation_geocoding",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "PRIMARY_COLUMN": "address",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": HERE_API_TOKEN,
            "DATASET_ID": "2",
        },
        volumes=[f"{DATA_DIR}:/app/data"],
        network_mode="app_network",
        docker_url=DOCKER_DAEMON_URL,
    )

    distance_calculation_pt = DockerOperator(
        task_id="distance_calculation_pt",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "public_transport",
            "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
            "OUTPUT_COLUMN": "distance_to_pt",
            "DATASET_ID": "2",
        },
        volumes=[f"{DATA_DIR}:/app/data"],
        network_mode="app_network",
        docker_url=DOCKER_DAEMON_URL,
    )

    distance_calculation_residential = DockerOperator(
        task_id="distance_calculation_residential",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "residential_areas",
            "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
            "OUTPUT_COLUMN": "distance_to_residential",
            "DATASET_ID": "2",
        },
        volumes=[f"{DATA_DIR}:/app/data"],
        network_mode="app_network",
        docker_url=DOCKER_DAEMON_URL,
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            "DATASET_ID": "2",
        },
        volumes=[f"{DATA_DIR}:/app/data"],
        network_mode="app_network",
        docker_url=DOCKER_DAEMON_URL,
    )

    # Pipeline execution order
    load_and_modify_data >> reconciliation_geocoding >> distance_calculation_pt >> distance_calculation_residential >> save_final_data