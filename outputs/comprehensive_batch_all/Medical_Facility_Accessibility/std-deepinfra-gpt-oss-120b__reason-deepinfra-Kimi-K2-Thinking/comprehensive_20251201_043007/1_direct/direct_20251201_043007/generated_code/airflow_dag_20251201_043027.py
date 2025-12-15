import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# Path to the shared data directory (mounted into containers)
DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")

# Common Docker settings
DOCKER_NETWORK = "app_network"
VOLUME_MOUNT = Mount(source=DATA_DIR, target="/app/data", type="bind", read_only=False)

with DAG(
    dag_id="medical_facility_accessibility",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Assess medical facility accessibility by geocoding and distance calculations.",
    tags=["medical", "accessibility"],
) as dag:

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        mounts=[VOLUME_MOUNT],
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
            "INPUT_CSV": "/app/data/facilities.csv",
            "OUTPUT_JSON": "/app/data/table_data_2.json",
        },
        command="python /app/run.py",  # Adjust if the container expects a specific entrypoint
    )

    reconciliation = DockerOperator(
        task_id="reconciliation_geocoding_here",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        mounts=[VOLUME_MOUNT],
        environment={
            "DATASET_ID": "2",
            "PRIMARY_COLUMN": "address",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": os.getenv("HERE_API_TOKEN", "YOUR_HERE_TOKEN"),
            "INPUT_JSON": "/app/data/table_data_2.json",
            "OUTPUT_JSON": "/app/data/reconciled_table_2.json",
        },
        command="python /app/run.py",
    )

    distance_pt = DockerOperator(
        task_id="distance_to_public_transport",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        mounts=[VOLUME_MOUNT],
        environment={
            "DATASET_ID": "2",
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "public_transport",
            "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
            "OUTPUT_COLUMN": "distance_to_pt",
            "INPUT_JSON": "/app/data/reconciled_table_2.json",
            "OUTPUT_JSON": "/app/data/distance_pt_2.json",
        },
        command="python /app/run.py",
    )

    distance_residential = DockerOperator(
        task_id="distance_to_residential_areas",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        mounts=[VOLUME_MOUNT],
        environment={
            "DATASET_ID": "2",
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "residential_areas",
            "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
            "OUTPUT_COLUMN": "distance_to_residential",
            "INPUT_JSON": "/app/data/distance_pt_2.json",
            "OUTPUT_JSON": "/app/data/column_extended_2.json",
        },
        command="python /app/run.py",
    )

    save_final = DockerOperator(
        task_id="save_final_csv",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        mounts=[VOLUME_MOUNT],
        environment={
            "DATASET_ID": "2",
            "INPUT_JSON": "/app/data/column_extended_2.json",
            "OUTPUT_CSV": "/app/data/enriched_data_2.csv",
        },
        command="python /app/run.py",
    )

    # Define task dependencies
    load_and_modify >> reconciliation >> distance_pt >> distance_residential >> save_final